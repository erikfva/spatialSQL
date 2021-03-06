SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_test(opt json DEFAULT '{}'::json)
 RETURNS TABLE(test character varying, result character varying, descripcion character varying)
 LANGUAGE plpgsql
AS $function$
DECLARE 
    var_r record;
    sql text;
    res text;
    condition text;
BEGIN
	sql := 'SELECT * FROM registro_derecho.pruebas';
    condition := CASE WHEN COALESCE((opt->>'test')::text, '') <>'' THEN ('(test = '''|| (opt->>'test')::text || ''')') ELSE '(TRUE)' END;
	sql := sql || ' WHERE ' || condition;    
 FOR var_r IN EXECUTE sql  
 LOOP
 	test := var_r.test; 
    descripcion := var_r.descripcion;
    sql := var_r.entrada;
    EXECUTE sql INTO res;
    result := CASE 
    	WHEN upper(var_r.tipo) = 'JSON' 
        AND (
        		SELECT 
            	NOT exists(
            		SELECT
                	FROM json_each_text((var_r.salida)::json) t1
                		FULL OUTER JOIN json_each_text((res)::json) t2 USING (key)
                	WHERE t1.value<>t2.value OR t1.key IS NULL OR t2.key IS NULL                
				)
        ) THEN
        	'OK'
        ELSE
        	'ERROR!!'
    END;
    RETURN NEXT;
 END LOOP;
END;
$function$
 