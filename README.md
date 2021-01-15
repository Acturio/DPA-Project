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
