import random
import time
from shapely.geometry import Point, Polygon
from pyspark.sql import SparkSession
import json
from shapely.wkb import loads
from shapely.geometry import mapping
from pyspark.sql.functions import col, udf, from_json
from pyspark.sql.types import StringType, StructType, StructField, ArrayType, DoubleType
from datetime import datetime
import uuid
import time
import json
import os
from datetime import datetime
from databricks.sdk import WorkspaceClient
import io
import os
import yaml
from typing import Dict
import logging
import pytz

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    # stream=sys.stdout 
)

DATABRICKS_HOST = os.getenv('DATABRICKS_HOST')
DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN')

# Definir cliente de Databricks
w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)

# Configuraciones 
CONFIG_FILE = "/app/config/producer_configs.yaml"
medellin_data_path = "/app/data/50001.parquet"
customer_data_path = "/app/data/customers.parquet"
employees_data_path = "/app/data/employees.parquet"
config = {}
last_mtime = None

def cargar_configuracion():
    """Carga la configuración desde el archivo YAML."""
    global config, last_mtime
    try:
        current_mtime = os.path.getmtime(CONFIG_FILE)
        
        if current_mtime == last_mtime:
            return

        logging.info("Se detectó cambio en config.yaml, Recargando configuración...")
        with open(CONFIG_FILE, 'r') as f:
            config = yaml.safe_load(f)
        
        last_mtime = current_mtime
        logging.info("Configuración recargada:", config)

        return config

    except FileNotFoundError:
        logging.info(f"Error: El archivo '{CONFIG_FILE}' no se encuentra.")
        config = {}
    except Exception as e:
        logging.info(f"Error al cargar la configuración: {e}")


def subir_datos(registro: Dict, dbfs_path: str) -> None:
    # Convertir a bytes
    json_bytes = json.dumps(registro).encode('utf-8')
    BinaryIO = io.BytesIO(json_bytes)

    logging.info("Subiendo datos a DBFS ...")

    # Subir el archivo a DBFS
    w.dbfs.upload(path=dbfs_path, src=BinaryIO, overwrite=True)
    
    logging.info("Subida completada")


def wkb_to_geojson(wkb_list_of_ints):
    """
    Decodifica una geometría que Spark ha interpretado como una lista de enteros
    (originalmente en formato WKB) y la convierte a un string GeoJSON.
    """
    if wkb_list_of_ints is None:
        return None
    try:
        wkb_bytes = bytes(wkb_list_of_ints)
        geom = loads(wkb_bytes)
        geom_dict = mapping(geom)
        return json.dumps(geom_dict)
    except Exception as e:
        logging.info(f"Error al procesar la geometría: {e}")
        return None
    

def cargar_datos_iniciales():
    
    # Iniciar sesión de Spark
    spark = SparkSession.builder \
        .appName("LeerParquetLocal") \
        .master("local[*]") \
        .getOrCreate()

    logging.info("Cargando datos iniciales...")

    # Extraer coordenadas
    df_medellin = spark.read.parquet(medellin_data_path)

    wkb_to_geojson_udf = udf(wkb_to_geojson, StringType())

    geojson_schema = StructType([
        StructField("type", StringType(), True),
        StructField("coordinates", ArrayType(ArrayType(ArrayType(DoubleType()))), True)
    ])
    
    df_with_geojson = df_medellin.withColumn("geojson_str", wkb_to_geojson_udf(col("geometry")))
    df_parsed = df_with_geojson.withColumn("geojson_parsed", from_json(col("geojson_str"), geojson_schema))

    geometry_struct = col("geojson_parsed").alias("geometry")

    coordinates = df_parsed.select(geometry_struct).first()
    coordinates_dict = coordinates.asDict(recursive=True)

    # Extraer IDs de los clientes
    df_customer = spark.read.parquet(customer_data_path)
    customers_ids = [row['customer_id'] for row in df_customer.collect()]

    # Extraer IDs de los empleados
    df_employees = spark.read.parquet(employees_data_path)
    employees_ids = [row['employee_id'] for row in df_employees.collect()]

    # Deteniendo la sesión de Spark
    spark.stop()

    logging.info("Datos iniciales cargados")

    return customers_ids, employees_ids, coordinates_dict["geometry"]["coordinates"]


def generar_punto_aleatorio_en_poligono(polygon):
    """
    Genera un punto (lat, lon) aleatorio dentro del polígono.
    """
    min_x, min_y, max_x, max_y = polygon.bounds
    while True:
        random_point = Point(random.uniform(min_x, max_x), random.uniform(min_y, max_y))
        if random_point.within(polygon):
            return random_point


def generar_registro(ids_clientes, ids_empleados, coordinates):
    """
    Crea un único registro ficticio combinando los datos aleatorios.
    """

    # Seleccionar IDs aleatorios de las listas de IDs
    id_cliente_aleatorio = random.choice(ids_clientes)
    id_empleado_aleatorio = random.choice(ids_empleados)
    
    # Generar ubicación aleatoria
    poligono = Polygon(coordinates[0])
    punto_aleatorio = generar_punto_aleatorio_en_poligono(poligono)

    # Establecer la zona horaria de Bogotá
    zona_bogota = pytz.timezone('America/Bogota')
    # Obtener fecha-hora actual en Bogotá
    fecha_formateada = datetime.now(zona_bogota).strftime("%d/%m/%Y %H:%M:%S") # Formatear "12/05/2024 10:43:19"

    # Generar número aleatorio
    cantidad = random.randint(1, 100)
    
    id_unico = str(uuid.uuid4())

    registro = {
        "latitude": punto_aleatorio.y,
        "longitude": punto_aleatorio.x,
        "date": fecha_formateada,
        "customer_id": id_cliente_aleatorio,
        "employee_id": id_empleado_aleatorio,
        "quantity_products": cantidad,
        "order_id": id_unico
        }
    
    return registro


if __name__ == "__main__":
    
    cargar_configuracion()

    ids_clientes, ids_empleados, poligono = cargar_datos_iniciales()
    
    logging.info("\nIniciando generador de datos ...")
    
    try:
        while True:

            # Actualizar configuraciones
            cargar_configuracion()

            # Obtener configuraciones
            dbfs_dest_path_base = config.get("dbfs_dest_path_base", "/Volumes/workspace/bronze/staging/sales/")
            upload_interval_seconds = config.get("upload_interval_seconds", 30)

            # Genera un nuevo registro
            nuevo_registro = generar_registro(ids_clientes, ids_empleados, poligono)
            
            # Establecer la zona horaria de Bogotá
            zona_bogota = pytz.timezone('America/Bogota')
            # Obtener fecha-hora actual en Bogotá
            fecha_hora = datetime.now(zona_bogota).strftime("%Y%m%d_%H%M%S")
        
            # Construir path en DBFS
            dbfs_file_name = f"data_{fecha_hora}.json"
            dbfs_full_path = os.path.join(dbfs_dest_path_base, dbfs_file_name)

            # Subir datos a DBFS
            subir_datos(nuevo_registro, dbfs_full_path)
            
            # Espera antes de la siguiente iteración
            time.sleep(upload_interval_seconds)
            logging.info(f"\nPróxima ejecución en {upload_interval_seconds} segundos...")
            
    except KeyboardInterrupt:
        logging.info("\nGenerador detenido por el usuario.")