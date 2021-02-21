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
- Actualizar el repositorio (si ya se tiene) `git pull origin main`

- Instalar los paquetes descritos en el archivo requirements.txt `pip install -r requirements.txt`

- Obtener token de la API de Food inspections

- Tener las credenciales de AWS (Access key ID y Secret access key) dados por el administrador de la indfaestructura en AWS, y guardarlos en un archivo credentials.yml y guardarlos dentro de la carpeta conf/local(credentials.yml), con la siguiente estructura:


```
---
s3:
   aws_access_key_id: "TU_ACCESS_KEY_IN"
   aws_secret_access_key: "SECRET_ACCESS_KEY"
   
food_inspections: 
   api_token: "TU_TOKEN" 
```

- Para probar las funciones de los archivos `src/utils/general.py` y `src/pipeline/ingesta_almacenamiento.py`, es necesario estar ubicado en la raíz del proyecto.

- Ejecutar `export PYTHONPATH=$PWD`

- Utilizar un ambiente virtual con python superior o igual a 3.7.4., ejemplo: `pyenv activate nombre_de_tu_ambiente`

- Se ejecuta `python`

- Para probar `src/utils/general.py` se ejecuta: 

  `from src.utils.general import read_yaml_file, get_s3_credentials, get_api_token`
  
  para cargar la librería.
  
- Con el siguiente comando se lee un archivo `.yml`, regresando el contenido en la variable `ry` para visualizarlo.

```
ry = read_yaml_file("conf/local/credentials.yaml")
ry
```
  
- Con el siguiente comando se obtienen las credenciales S3, regresando el contenido en la variable `s3_c` para visualizarlo.

```
s3_c = get_s3_credentials("conf/local/credentials.yaml")
s3_c
```
  
- Con el siguiente comando se obtiene el token, regresando el contenido en la variable `token` para visualizarlo.

```
token = get_api_token("conf/local/credentials.yaml")
token
```
- Para probar `src/pipeline/ingesta_almacenamiento.py` se ejecuta: 

  `from src.pipeline.ingesta_almacenamiento import get_client, ingesta_inicial, get_s3_resource, guardar_ingesta, ingesta_consecutiva`
  
  para cargar la librería.

- Para obtener el cliente se usa la función `get_client` como sigue:
```
cliente = get_client()
```

- Para realizar la obtención de datos de la ingesta inicial, se implementa la función `ingesta_inicial` que recibe como parámetros un cliente y el límite de datos, como se indica a continuación.

```
datos_his = ingesta_inicial(cliente, 300000)
```

- Para obtener el recurso de S3, se usa la función `get_s3_resource` y se implementa como sigue

```
s3_resource = get_s3_resource()
```

- Se declaran las siguientes variables, con el nombre del bucket y los paths donde se guardará la ingesta inicial y consecutiva.

```
bucket = "data-product-architecture-equipo-n"
bucket_path_hist = "ingestion/initial/historic-inspections-"
bucket_path_cons = "ingestion/consecutive/consecutive-inspections-"
```

- Para guardar la ingesta se usa la función `guardar_ingesta`, que recibe como parámetros el nombre del bucket, el path y los datos, se implementa como sigue:

```
guardar_ingesta(bucket, bucket_path_hist, datos_his)
```

- Para obtener los datos consecutivos se usa la función `ingesta_consecutiva`, que recibe como parámetros el cliente, una fecha a partir de donde se extraerán los datos y el límite de los mismos, se podrá llamar de la siguiente manera:

```
datos_cons = ingesta_consecutiva(cliente, '2021-01-01', 2000)
```

Si se le pasa `None` al segundo parámetro (fecha), restará al día actual 7 días para obtener los datos de una semana atrás.

- Finalmente se puede usar nuevamente la función `guardar_ingesta` para cargar en el bucket los datos consecutivos.

```
guardar_ingesta(bucket, bucket_path_cons, datos_cons)
```







