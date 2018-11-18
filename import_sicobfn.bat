@echo off
call config.bat
REM ********************************************************************************
REM **Importando las funciones espaciales de geoSICOB**
REM ********************************************************************************

for %%i in (*.sql) do (
	REM echo %sicobfn_dir%%%i
	psql -h %pghost% -p %pgport% -U %pguser% -d %pgdb% -f "%%i"
)
