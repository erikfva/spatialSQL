SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_execute_geoprocess()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE 
 sql TEXT;
 entrada json;
 salida json;
BEGIN
    entrada := NEW.entrada::jsonb || jsonb_build_object('idgeoproceso', NEW.idgeoproceso); 
	sql := Format('SELECT %s(''%s'') AS salida', NEW.proceso, entrada);
        	
	RAISE DEBUG 'geoprocesamiento_tr_au: %, %', pg_trigger_depth(), sql;
	IF pg_trigger_depth() = 1 THEN
    	EXECUTE sql INTO salida;
    END IF;
    /*
    PERFORM sicob_obtener_predio('{"lyr_in": "processed.f20160708agfbdecf223329c_nsi", "idgeoproceso": 1}');
    */
    
    /*UPDATE registro_derecho.geoprocesamiento SET debug =  sql  WHERE idgeoproceso = NEW.idgeoproceso;*/
    RETURN NEW;
    /*
	EXCEPTION
	WHEN others THEN
        PERFORM sicob_log_register( ('{"idgeoproceso":"' || (entrada->>'idgeoproceso')::text || '", "exec_point":"fin","msg":{"error":"SQLERRM"}}')::json );
      
        RAISE EXCEPTION 'geoprocesamiento_tr_au : entrada: % >> %, (%)', pg_trigger_depth(), SQLERRM, SQLSTATE;
      */  
END;
$function$
 