SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_ejecutar_geoproceso(_info json)
 RETURNS json
 LANGUAGE plpgsql
AS $function$
DECLARE
    _proceso varchar;
	_entrada json;
    _salida json;
	_idgeoproceso varchar;
	_sql varchar;
BEGIN
 
	_idgeoproceso := COALESCE((_info->>'idgeoproceso')::text,'-1'); 
    BEGIN /* inicio de 1er bloque para control de errores.*/
	    SELECT g.proceso, g.entrada INTO STRICT _proceso, _entrada
	    FROM  registro_derecho.geoprocesamiento g
	    WHERE g.idgeoproceso = _idgeoproceso::integer;
	   -- _sql :=  'SELECT * FROM ' || _proceso || '('''|| _entrada || '''::json) AS _out';
        _sql :=  'SELECT ' || _proceso || '('''|| _entrada || ''')';
        RAISE DEBUG 'Running %', _sql;
	    PERFORM sicob_log_geoproceso( ('{"idgeoproceso":"' || _idgeoproceso || '", "exec_point":"inicio"}')::json );

        BEGIN
        	EXECUTE _sql INTO _salida;
        EXCEPTION
    		WHEN others THEN
            _salida := ('{"success":"0","msg":' || to_json('sicob_ejecutar_geoproceso: ' || regexp_replace(regexp_replace(SQLERRM, '\t|\r\n|''', '', 'g'),'\s+' ,' ' ,'g') ) || '}')::json;
            RAISE WARNING 'sicob_ejecutar_geoproceso: % - %', SQLERRM, SQLSTATE;
        END;

	EXCEPTION
    	WHEN others THEN
        _salida := ('{"success":"0","msg":' || to_json('sicob_ejecutar_geoproceso: ' || regexp_replace(regexp_replace(SQLERRM, '\t|\r\n|''', '', 'g'),'\s+' ,' ' ,'g') ) || '}')::json;
	     RAISE WARNING 'sicob_ejecutar_geoproceso: % - %', SQLERRM, SQLSTATE;
	END; /*fin de 1er bloque de control de errores.*/

	PERFORM sicob_log_geoproceso( ('{"idgeoproceso":"' || _idgeoproceso || '", "exec_point":"fin","payload":' || _salida || '}')::json );    

    RETURN _salida;
END;
$function$
 