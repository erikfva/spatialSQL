@echo off
call config.bat

for /f %%A in ('psql -h %%pghost%% -p %%pgport%% -U %%pguser%% -d %%pgdb%% -t -c "SELECT proname FROM pg_proc p LEFT OUTER JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname ~ 'public' AND p.proname ~ 'sicob_'  ORDER BY proname"') do ( 
		REM @echo %%A > "%%A.sql"
		psql -Atz0 -h %pghost% -p %pgport% -U %pguser% -d %pgdb% -c "SELECT string_agg(t.definition, ';') as sqlscript FROM (SELECT pg_get_functiondef(p.oid) AS definition FROM pg_proc p INNER JOIN pg_type t ON (p.prorettype = t.oid) LEFT OUTER JOIN pg_description d ON (p.oid = d.objoid) LEFT OUTER JOIN pg_namespace n ON (n.oid = p.pronamespace) WHERE n.nspname ~ 'public' AND proname = '%%A' ORDER BY proname)t" -o %%A.sql
) 
