@echo off
call config.bat
REM ********************************************************************************
REM **Importando las funciones espaciales de geoSICOB**
REM ********************************************************************************
for %%i in (*.sql) do (
	REM echo %sicobfn_dir%%%i
	psql -h %pghost% -p %pgport% -f "%%i" postgresql://%pguser%:%pgpsw%@localhost/%pgdb%
)
set /p pgpswadmin=Introduzca la clave del usuario postgres? 
psql -p %pgport% -f "sicob_tr_after_create_table.sql" postgresql://postgres:%pgpswadmin%@%pghost%/%pgdb%