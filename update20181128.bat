@echo off
call config.bat
psql -U postgres -d %pgdb% -c "CREATE EXTENSION postgis_topology; grant usage on schema topology to admderechos; grant all on all tables in schema topology to admderechos; grant usage, select on all sequences in schema topology to admderechos;"

REM ********************************************************************************
REM **Importando las funciones espaciales de geoSICOB**
REM ********************************************************************************
psql -h %pghost% -p %pgport% -U %pguser% -d %pgdb% -f "sicob_fix_topo.sql"

