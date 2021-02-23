# DPA-Project
Proyecto enfocado al desarrollo de un producto de datos para predecir la acreditación de inspecciones de restaurantes en Chicago

### Integrantes del equipo

| Nombre |
| :------- |
| Karina Lizette Gamboa Puente|
| Miguel López Cruz|
| Oscar Arturo Bringas López|
| Aide Jazmín González Cruz|

### Resumen de  datos con los que se trabajaran

- Número de registros: **215K**
- Número de columnas: **17**
- ¿Qué variables son y qué información tiene?

| Variable | Tipo  | Descripción |
| :------- | :----:| :---------: |
|Inspection ID|Number|Identificador de la inspección|
|DBA Name|Text|Nombre del establecimiento|
|AKA Name|Text|Alias (también conocido como)|
|License #|Number|Número de licencia del establecimiento|
|Facility Type|Text|Tipo de establecimiento|
|Risk|Text|Riesgo de cometer una violación|
|Address|Text|Dirección del establecimiento|
|City|Text|Ciudad donde está el establecimiento|
|State|Text|Estado donde se encuentra el establecimiento|
|Zip|Number|Código postal del establecimiento|
|Inspection Date|Floating Timestamp|Fecha de la inspección|
|Inspection Type|Text|Tipo de inspección|
|Results|Text|Resultado de la inspección|
|Violations|Text|Observaciones de las violaciones encontradas|
|Latitude|Number|Latitud del establecimiento|
|Longitude|Number|Longutud del establecimiento|
|Location|Location|Latitud y Longitud (geopoint)|
    
### Pregunta analítica a contestar con el modelo predictivo

- ¿El establecimiento pasará o no la inspección?

### Frecuencia de actualización de los datos

- La frecuencia de datos fuente es diaria, pero en este proyecto se realizará semanalmente de acuerdo a los requerimientos establecidos.


### Requerimientos: 
- Actualizar el repositorio (si ya se tiene) `git pull origin main` o clonarlo en caso de que no.

- Instalar los paquetes descritos en el archivo requirements.txt `pip install -r requirements.txt`

- Obtener token de la API de Food inspections. Se puede obtener un token [aquí](https://data.cityofchicago.org/profile/edit/developer_settings)

- Se debe contar con credenciales de AWS (Access key ID y Secret access key) dados por el administrador de la infraestructura en AWS, y guardarlos en un archivo `credentials.yml`. El archivo con las credenciales debe estar en la carpeta `conf/local/credentials.yml`. La estructura del archivo `credentials.yml` debe tener una estructura como la que se presenta a continuación:


```
---
s3:
   aws_access_key_id: "TU_ACCESS_KEY_ID"
   aws_secret_access_key: "SECRET_ACCESS_KEY"
   
food_inspections: 
   api_token: "TU_TOKEN" 
```

- Para ejecutar las funciones del proyecto, es importante ubicarse en la raíz del proyecto y seguir las siguientes instrucciones en la línea de comandos:

- Ejecutar `export PYTHONPATH=$PWD`

- Utilizar un ambiente virtual con python superior o igual a 3.7.4., ejemplo: `pyenv activate nombre_de_tu_ambiente`

- Ejecutar: `python`

- Importar las librerías del módulo `src/utils/general.py` a través de los siguiente comandos: 

  `from src.utils.general import read_yaml_file, get_s3_credentials, get_api_token`
    
- Con el siguiente comando se lee un archivo `.yml`, el cual encuentra las credenciales introducidas en el archivo yaml.

```
ry = read_yaml_file("conf/local/credentials.yaml")
```
  
- Con el siguiente comando se obtienen las credenciales S3, las cuales se obtienen también a través del archivo yaml creado anteriormente.

```
s3_c = get_s3_credentials("conf/local/credentials.yaml")
```
  
- A continuación, se obtiene el token de acceso a la API, mismo que servirá para poder descargar los datos.

```
token = get_api_token("conf/local/credentials.yaml")
```
- Para cargar las librerías que permiten realizar la esctracción de datos y almacenamiento, se ejecutan los siguientes comandos: 

  `from src.pipeline.ingesta_almacenamiento import get_client, ingesta_inicial, get_s3_resource, guardar_ingesta, ingesta_consecutiva`
  
  `import src.utils.constants as cte`
  
  esta libería usa internamente un archivo de constantes, el cual esta en la ruta `src/utils/constants.py` y contiene la siguiente información
  
```
# The Host Name for the API endpoint (the https:// part will be added automatically)
DATA_URL = 'data.cityofchicago.org'
# The data set at the API endpoint
DATA_SET = '4ijn-s7e5'
# Nombre del bucket
BUCKET = "data-product-architecture-equipo-n"
# Path de ingesta histrórica
BUCKET_PATH_HIST = "ingestion/initial/historic-inspections-"
# Path de ingesta consecutiva
BUCKET_PATH_CONS = "ingestion/consecutive/consecutive-inspections-"
```

estos valores se pueden sustiruir pos los propios (nombre de bucket y paths) para probar la ingestión de datos.
  
- Para obtener el cliente se usa la función `get_client`, agregando el token que anteriormente fue generado. La implementación se muestra a continuación:

```
cliente = get_client(token)
```

- Para realizar la obtención de datos de la ingesta inicial, se implementa la función `ingesta_inicial` que recibe como parámetros un cliente y el límite de datos, como se indica a continuación:

```
datos_his = ingesta_inicial(cliente, 300000)
```

- Para obtener el recurso de S3, se usa la función `get_s3_resource` y se implementa como sigue:

```
s3_resource = get_s3_resource()
```

- Se declaran las siguientes variables, con el nombre del bucket y los paths donde se guardará la ingesta inicial y consecutiva.

```
bucket = cte.BUCKET
bucket_path_hist = cte.BUCKET_PATH_HIST
bucket_path_cons = cte.BUCKET_PATH_CONS
```

- Para guardar la ingesta se usa la función `guardar_ingesta`, que recibe como parámetros el nombre del bucket, el path y los datos, se implementa como sigue:

```
guardar_ingesta(bucket, bucket_path_hist, datos_his)
```

- Para obtener los datos consecutivos se usa la función `ingesta_consecutiva`, que recibe como parámetros el cliente, una fecha a partir de donde se extraerán los datos y el límite de los mismos, se podrá llamar de la siguiente manera:

```
datos_cons = ingesta_consecutiva(cliente, '2021-02-14', 2000)
```

Si se le pasa `None` al segundo parámetro (fecha), restará al día actual 7 días para obtener los datos de una semana atrás.

- Finalmente se puede usar nuevamente la función `guardar_ingesta` para cargar en el bucket los datos consecutivos.

```
guardar_ingesta(bucket, bucket_path_cons, datos_cons)
```

Una vez que este comando ha finalizado, puede dirigirse al bucket S3 de AWS en donde se realizó la ingesta de los datos para verificar que efectivamente han sido almacenados satisfactoriamente.






