set PATH_OGR2OGR=C:\OSGeo4W64\bin\ogr2ogr.exe
set PATH_GDB=\\abt-sdc\imgenes procesadas\ABT_COBERTURAS\Geodatabase.gdb
set pghost=192.168.50.9
set pgport=5432
set pgdb=geodatabase
set pguser=admderechos
REM ******************
REM * El password para el usuario "pguser" debe configurarse en el archivo .pgpass del sistema.
REM * On Microsoft Windows the file is named %APPDATA%\postgresql\pgpass.conf (where %APPDATA% refers to the Application Data subdirectory in the user's profile).
REM * El formato es -> hostname:port:database:username:password
REM ******************
set pgschema=coberturas
set pgsrid=3003
set pggeom=the_geom
REM set pgencoding="UTF-8"
SETLOCAL ENABLEEXTENSIONS EnableDelayedExpansion