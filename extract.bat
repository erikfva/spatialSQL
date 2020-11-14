@echo off
call config.bat

psql -t -c "SELECT proname FROM pg_proc p LEFT OUTER JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname ~ 'public' AND p.proname ~ 'sicob_' ORDER BY proname" postgresql://%pguser%:%pgpsw%@%pghost%/%pgdb% > "functionList.txt"

for /f %%A in (functionList.txt) do ( 
		REM @echo %%A > "%%A.sql"
		psql -Atz0 -c "\encoding [UTF8]" -c "SELECT 'SET CLIENT_ENCODING TO ''utf8'';' || chr(10) || replace(string_agg(t.definition, ';'), E'\r','') as sqlscript FROM (SELECT pg_get_functiondef(p.oid) AS definition FROM pg_proc p INNER JOIN pg_type t ON (p.prorettype = t.oid) LEFT OUTER JOIN pg_description d ON (p.oid = d.objoid) LEFT OUTER JOIN pg_namespace n ON (n.oid = p.pronamespace) WHERE n.nspname ~ 'public' AND proname = '%%A' ORDER BY proname)t" -o "%%A.sql" postgresql://%pguser%:%pgpsw%@%pghost%/%pgdb%
) 
