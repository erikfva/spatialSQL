# spatialSQL
Funciones geográficas escritas en PL/pgSQL de PostgreSQL utilizando funciones de la extensión PostGIS.
## Instalación
1. Edite el archivo **config.bat** y configure los parametros de la base de datos y ubiación de archivos.
2. Importe/actualize las funciones ejecutando el script **import_fn.bat** 
3. Conectese mediante el usuario superadministrador **postgis** y ejecute el script **sicob_tr_after_create_table.sql**.
4. (Solo para uso de procesamiento paralelo), edite la funcion **sicob_paralellsql** y cambie los valores de coneccion de la variable **connectstring**.
