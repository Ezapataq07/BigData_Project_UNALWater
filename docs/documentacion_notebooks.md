


## Estructura del Proyecto

Este proyecto emplea una arquitectura en medalla, que organiza el procesamiento de datos en tres zonas principales:

- **Bronze**: Zona de ingesta y almacenamiento de datos crudos, donde se realiza la limpieza inicial. Los datos se almacenan en formato Delta Lake para asegurar transacciones ACID y versionado.
- **Silver**: Zona de transformación e integración, donde los datos se enriquecen, se validan y se integran con otras fuentes. El formato Delta se mantiene para facilitar consultas eficientes y actualizaciones seguras.
- **Gold**: Zona de agregación y generación de datasets finales, listos para análisis avanzado, reportes y modelado predictivo. Los datos siguen en formato Delta, optimizando el acceso y la trazabilidad.


La arquitectura en medalla permite un flujo de datos claro, seguro y escalable, facilitando la trazabilidad y el mantenimiento.

---


# Notebooks de Procesamiento de Datos (`data`)

## Zona Bronze

### brz_customers.ipynb
Realiza la carga inicial de datos de clientes desde fuentes externas. Incluye procesos de limpieza, validación y transformación básica usando PySpark, preparando los datos para etapas posteriores.

### brz_employees.ipynb
Gestiona la ingesta y depuración de datos de empleados. Aplica transformaciones para estandarizar la información y asegurar su calidad antes de integrarla con otros conjuntos de datos.

### brz_municipalities.ipynb
Procesa datos de municipios, realizando tareas de limpieza y formateo. Permite la integración de datos geográficos y administrativos relevantes para el análisis territorial.

### brz_neighborhoods.ipynb
Trata datos de barrios, asegurando su correcta estructuración y depuración. Facilita la vinculación con datos de municipios y clientes para análisis espaciales.

### brz_sales.ipynb
Administra la ingesta de registros de ventas en modo streaming, leyendo datos en tiempo real y almacenándolos en la zona bronze en formato Delta Lake. Realiza limpieza y validación inicial, preparando los datos para su procesamiento posterior.

---

## Zona Silver

### slv_customers.ipynb
Enriquece y transforma los datos de clientes, integrando información adicional y generando variables derivadas. Prepara los datos para análisis más detallados y segmentación.

### slv_employees.ipynb
Integra datos de empleados con otras fuentes, realiza validaciones cruzadas y genera métricas relevantes para el análisis de recursos humanos.

### slv_municipalities.ipynb
Fusiona datos municipales con información externa, permitiendo análisis comparativos y generación de indicadores territoriales.

### slv_neighborhoods.ipynb
Enlaza datos de barrios con otras entidades, genera variables de contexto y facilita el análisis de patrones espaciales y demográficos.

### slv_sales.ipynb
Procesa los datos de ventas recibidos en streaming desde la zona bronze, aplicando transformaciones y enriquecimiento. Integra información de clientes, empleados y territorios, y almacena los resultados en la zona silver en formato Delta Lake para análisis avanzados y reportes.

---

## Zona Gold

### gld_sales.ipynb
Agrega y transforma los datos de ventas provenientes de la zona silver, generando datasets finales optimizados para análisis estadístico y modelado predictivo. El procesamiento se realiza sobre datos en formato Delta Lake, asegurando eficiencia y trazabilidad. Incluye cálculos de indicadores clave y preparación para modelos de machine learning.

---

# Notebooks de Configuración y Utilidades (`config`)

### artifacts.ipynb
En este notebook se definen y documentan los artefactos principales del proyecto, incluyendo los esquemas de las tablas y vistas para cada zona de la arquitectura medallion (bronze, silver y gold). Aquí se establecen las estructuras de datos, los tipos de columnas y las relaciones necesarias para garantizar la correcta organización y trazabilidad de la información a lo largo del flujo de procesamiento. Además, se gestionan rutas, parámetros y recursos compartidos entre notebooks, facilitando la reutilización y la integración en todo el proyecto.

### utils.ipynb
Contiene funciones utilitarias en Python que apoyan tareas comunes como transformaciones, validaciones y manejo de errores. Facilita la reutilización de código y la estandarización de procesos.

### variables.ipynb
Centraliza la definición de variables globales y parámetros clave del proyecto. Permite modificar configuraciones de manera sencilla y consistente en todos los notebooks.

---

> **Nota:** Este proyecto sigue una arquitectura de datos moderna, donde cada notebook cumple una función específica en el flujo de procesamiento, asegurando calidad, trazabilidad y facilidad de mantenimiento.
