SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_create_triggers(table_name text)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
  sql TEXT;
BEGIN
--AGREGA EL EVENTO EN LA TABLA PARA ACTUALIZAR EL CONTENIDO DE LOS CAMPOS DE 
--INFORMACION GEOGRAFICA AUTOMATICAMENTE AL INSERTAR O ACTUALIZAR UN REGISTRO.
------------------------------------------------------------------------------
-- "update_the_geom_webmercator"
  sql := Format('DROP TRIGGER IF EXISTS sicob_tr_update_geoinfo_column ON %s', table_name);
  EXECUTE sql;
  
--Verificando si ya se ha agregado el trigger
/*        IF EXISTS(select 1
from pg_trigger
where not tgisinternal
and tgrelid = r.object_identity::regclass
and tgname = 'sicob_tr_update_geoinfo_column') THEN
			RETURN;
		END IF; 
 */         
  -- TODO: Why not AFTER?
  sql := 'CREATE trigger SICOB_tr_update_geoinfo_column BEFORE INSERT OR UPDATE OF the_geom ON '
         || table_name
         || ' FOR EACH ROW EXECUTE PROCEDURE SICOB_update_geoinfo_column()';
  EXECUTE sql;
END;
$function$
 