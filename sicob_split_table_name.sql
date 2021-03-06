SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_split_table_name(reloid text, OUT schema_name text, OUT table_name text)
 RETURNS record
 LANGUAGE plpgsql
 IMMUTABLE
AS $function$
  BEGIN
--SEPARA EL NOMBRE DEL ESQUEMA Y EL NOMBRE LA TABLA
-- Schema and relation names of a table given its reloid
-- Scope: private.
-- Parameters
--   reloid: oid of the table.
-- Return (schema_name, table_name)
-- note that returned names will be quoted if necessary

-- Prepare names to use in index and trigger names


	IF reloid::text LIKE '%.%' THEN
    	schema_name := regexp_replace (split_part(reloid::text, '.', 1),'"','','g');
    	table_name := regexp_replace (split_part(reloid::text, '.', 2),'"','','g');
    ELSE
        schema_name := '';
        table_name := regexp_replace(reloid::text,'"','','g');
    END IF;
/*
    select t.schemaname, t.relname INTO STRICT schema_name, table_name from 
  	(select split_part(reloid::text, '.', 1) as schemaname,split_part(reloid::text, '.', 2) as relname) t;
	IF table_name IS NULL OR table_name = '' THEN
    	table_name := schema_name;
        schema_name := 'public';
    END IF;
    */
  END
$function$
 