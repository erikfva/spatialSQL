SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_after_create_table()
 RETURNS event_trigger
 LANGUAGE plpgsql
AS $function$
DECLARE 
  r RECORD;
  exist_column BOOLEAN;
  sql TEXT;
BEGIN       
    --Verifiando si la tabla pertenece al esquema "uploads"
    SELECT * INTO r FROM pg_event_trigger_ddl_commands() WHERE command_tag in ('CREATE TABLE','ALTER TABLE');
    IF r.schema_name = 'uploads' THEN
    	        
	--Verificando si existe la columna the_geom en la tabla
	SELECT SICOB_exist_column(r.object_identity, 'the_geom') INTO exist_column;
	IF exist_column THEN
		-- Verificando si todavia no se ha creado la columna "sicob_id"
		SELECT SICOB_exist_column(r.object_identity, 'sicob_id') INTO exist_column;
		IF NOT exist_column THEN
			PERFORM sicob_create_id_column(r.object_identity);
		END IF;
            
		-- Verificando si todavia no se ha creado la columna "the_geom_webmercator"
		SELECT SICOB_exist_column(r.object_identity, 'the_geom_webmercator') INTO exist_column;
		IF NOT exist_column THEN
			PERFORM SICOB_add_geoinfo_column(r.object_identity);
			PERFORM SICOB_create_triggers(r.object_identity);
		END IF;
	END IF;
    -- RAISE NOTICE 'caught % event on % esquema %', r.command_tag, r.object_identity, r.schema_name;	
    -- RAISE DEBUG 'Running %', sql;     
    END IF;
END;
$function$