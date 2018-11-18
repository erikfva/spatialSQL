CREATE OR REPLACE FUNCTION public.sicob_executesql(_query text, _opt json)
 RETURNS json
 LANGUAGE plpgsql
AS $function$
DECLARE
    result jsonb;
    row_cnt integer;
    _table_name text;
    _schema text;
    _table_out text;
BEGIN
---------------------------
--PARAMETROS DE ENTRADA
---------------------------
--> _query : Consulta SQL a para ser ejecutada.
--> _opt : Parametros con las siguientes opciones:
	-- table_out : Nombre de la tabla resultante as ser creada a partid de la consulta "_query".
    -- temp : Por defecto es false. Indica si la tabla a ser creada sera temporal 
    --		  mientras dura la transaccion.
    -- return_scalar : Por defecto es false. Indica si la consulta devuelve un valor. 		  
    _query := replace(replace(_query,'=''','='''''),'''",','''''",');
    _table_out := COALESCE(_opt->>'table_out','');
    SELECT * FROM sicob_split_table_name(_table_out) INTO _schema, _table_name;
        
    IF _table_out <> '' THEN
    	IF COALESCE((_opt->>'temp')::boolean,FALSE) = TRUE THEN
        	EXECUTE 'DROP TABLE IF EXISTS ' || _table_name || ' CASCADE';
        	EXECUTE 'CREATE TEMPORARY TABLE ' || _table_name || ' ON COMMIT DROP AS ' || _query;
            result := jsonb_build_object('table_out', _table_name);
        ELSE
        	EXECUTE 'DROP TABLE IF EXISTS ' || _table_out || ' CASCADE';
        	EXECUTE 'CREATE UNLOGGED TABLE ' || _table_out || ' AS ' || _query; 
            result := jsonb_build_object('table_out', _table_out);
        END IF;
    ELSE
    	IF COALESCE((_opt->>'return_scalar')::boolean,FALSE) = TRUE THEN
        	EXECUTE 
            '
            WITH
            _fetchdata AS (' || _query || ' )
            SELECT row_to_json(t) FROM _fetchdata t
            ' INTO result;
        ELSE
    		EXECUTE _query;
        END IF;
    END IF;
	
    GET DIAGNOSTICS row_cnt = ROW_COUNT;
    result := result || jsonb_build_object('row_cnt', row_cnt);
    
    IF row_cnt > 0 AND _table_out <> '' AND COALESCE((_opt->>'create_index')::boolean,FALSE) = TRUE THEN
         
        
        IF COALESCE((_opt->>'temp')::boolean,FALSE) = TRUE THEN
        	EXECUTE 'DROP INDEX IF EXISTS ' || _table_name || '_sicob_id';
            EXECUTE 'CREATE INDEX ' || _table_name || '_sicob_id ON ' || _table_name || ' USING btree (sicob_id);'; 
        ELSE
        	EXECUTE 'ALTER TABLE ' || _table_out || ' ADD PRIMARY KEY (sicob_id);'; 
            EXECUTE 'DROP INDEX IF EXISTS ' || _table_name || '_geomid CASCADE';                 
            EXECUTE 'CREATE INDEX ' || _table_name || '_geomid
            ON ' || _table_out || ' 
            USING GIST (the_geom) ';             
        END IF;

          
   END IF;
 
   IF _table_out <> '' AND COALESCE((_opt->>'add_geoinfo')::boolean,FALSE) = TRUE THEN
		PERFORM sicob_add_geoinfo_column(_table_out);
        IF row_cnt > 0 THEN
        	PERFORM sicob_update_geoinfo_column(_table_out);
        END IF;
   END IF;
       
   RETURN result;

EXCEPTION WHEN others THEN
    RAISE EXCEPTION 'sicob_executesql: -> % , _query: %, _opt: %', SQLERRM, _query, _opt::text;
	
END;
$function$