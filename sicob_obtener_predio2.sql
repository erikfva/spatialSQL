CREATE OR REPLACE FUNCTION public.sicob_obtener_predio2(_opt json)
 RETURNS json
 LANGUAGE plpgsql
AS $function$
DECLARE 
 sql text;
 _out json := '{}';
 _tmp json;
 _tolerance text;
 _geojson text;
 _min_sup float;
 _row_cnt integer;
 tbl_name text;
 _lyr_pred text;
 _fldpredio text;
 _fldpropietario text;
 _condition text;
 a text;
 createdResult boolean := FALSE;
BEGIN

--VARIABLES DE ENTRADA:
--	lyr_in : capa con los poligonos que deben ser ubicados en sus predios correspondientes
--	condition (opcional):  Filtro para los datos de la capa "lyr_in". Si no se especifica, se toman todos los registros.
--> lyr_pred : capa con predios de referencia.
--> fldpredio : campo que contiene el nombre del predio en la capa de referencia.
--> fldpropietario : campo que contiene el nombre del propietario en la capa de referncia. 
--> geojson (opcional true/false): Devuelve el resultado en formato geojson. Por defecto es false.
--> min_sup (opcional): Superficie minima (en hectareas) permitida para los poligonos de la capa resultado.
--> tolerance (opcional): Distancia máxima (en metros) para el autoajuste automático de los bordes de "a" hacia los bordes de "b" (snapping). Si no se especifica, no se realiza autoajuste.

--VARIABLES DE SALIDA:
--> lyr_over: Nombre de la capa resultante, este es igual al nombre de la capa de entrada más el subfijo “_pred”. Esta capa siempre será creada en el esquema “processed” de la BD. 
--> features_inters_cnt: Cantidad de polígonos que SI se han encontrado sus predios.
--> features_diff_cnt: Cantidad de polígonos de los cuales NO se ha encontrado predio.
--> lyr_geojson : Si se ha indicado el valor geojson de entrada como "true", devuelve la capa resultado en formato geojson.
--> lyr_pred: Nombre de la capa que contiene los predios encontrados.

--_opt := '{"lyr_in":"processed.f20161118dgfaceb9e93b41b_nsi"}'::json;
--_opt := '{"lyr_in":"coberturas.b0603206_pdm_a052016","condition":"objectid_1 = 35884"}';
--_opt := '{"lyr_in":"coberturas.b0603206_pdm_a052016","condition":"objectid_1 = 11426"}';
--_opt := '{"lyr_in":"coberturas.pdm","condition":"res_adm = ''RS-OLSC-PDM-129-2000''"}';
_opt := '{"lyr_in":"uploads.f20170704gcfebdac5d7c097","lyr_pred":"uploads.f20170705ecgdbfafbc4c20f"}'::json;

_min_sup := COALESCE((_opt->>'min_sup')::real, 0.002); --> Esta superficie es la menor existente en la BD
_tolerance := COALESCE((_opt->>'tolerance')::text, '5.3');
tbl_name := (_opt->>'lyr_in')::text;
	SELECT (sicob_split_table_name(tbl_name)).table_name INTO tbl_name ;
_condition := COALESCE( (_opt->>'condition')::text , 'TRUE');

----------
--FASE 0 :
----------

--Realizando la sobreposicion con la capa de predios de referencia.
--El resultado es una nueva capa con el nombre de la cobertura de entrada mas el subfijo: "_pred".

_lyr_pred := COALESCE( (_opt->>'lyr_pred')::text,'coberturas.predios_titulados');
a := (_opt->>'lyr_in')::text;
/*
	IF COALESCE( (_opt->>'lyr_pred')::text,'') <> '' THEN --> Si se indica cobertura de referencia.
    	_out := sicob_overlap(('{"a":"' || a || '","condition_a":"' || _condition || '","b":"' || _lyr_pred || '","subfix":"_ref","tolerance":"' || _tolerance || '","add_diff":true,"temp" : true}')::json);             
        IF COALESCE( (_out->>'features_inters_cnt')::int,0) > 0 THEN
            _fldpredio := COALESCE((_opt->>'fldpredio')::text, 'predio');
            IF sicob_exist_column(_lyr_pred, _fldpredio) THEN
                _fldpredio := 'a.' || _fldpredio;
            ELSE
                _fldpredio := '''COLUMNA NO ENCONTRADA''';
            END IF;
            
            _fldpropietario := COALESCE((_opt->>'fldpropietario')::text, 'propietario');
            IF sicob_exist_column(_lyr_pred, _fldpropietario) THEN
                _fldpropietario := 'a.' || _fldpropietario;
            ELSE
                _fldpropietario := '''COLUMNA NO ENCONTRADA''';
            END IF;
            
            sql := format('
                    SELECT 
                    a.id_a,
                    a.source_a,
                    a.id_b,
                    a.source_b,            
                    CAST(%s AS text) as predio,
                    CAST(%s AS text) as propietario,
                    a.the_geom
                    FROM
                        %s a
                    WHERE a.id_b IS NOT NULL         
                    ',_fldpredio,_fldpropietario, (_out->>'lyr_over')::text);  
            RAISE DEBUG 'Running %', sql;
            EXECUTE 'DROP TABLE IF EXISTS processed.' || tbl_name || '_pred';
            EXECUTE 'CREATE TABLE processed.' || tbl_name || '_pred' || ' AS ' || sql;
            createdResult := TRUE;
        END IF;
        IF COALESCE( (_out->>'features_diff_cnt')::int,0) > 0 THEN --> si existen poligonos NO encontrados.
        	a := (_out->>'lyr_over')::text;
            _condition := 'id_b IS NULL';        
        ELSE
        	a := '';
        END IF;
    END IF;
  */ 
----------
--FASE 1 :
----------

--Realizando la sobreposicion con la capa de predios titulados.
--El resultado es una nueva capa con el nombre de la cobertura de entrada mas el subfijo: "_pred".    
_lyr_pred := 'coberturas.predios_titulados';
	IF a <> '' THEN --> Si existen poligonos para procesar.
		_out := sicob_overlap(('{"a":"' || a || '","condition_a":"' || _condition || '","b":"' || _lyr_pred || '","subfix":"_tit","tolerance":"' || _tolerance || '","add_diff":true,"temp":true}')::json);
        IF COALESCE( (_out->>'features_inters_cnt')::int,0) > 0 THEN --> Si se han encontrado predios.
        	IF a <> (_opt->>'lyr_in')::text THEN --> Actualizar referencia de la tabla resultado.
                -->Cambiando la referencia de "a" hacia la tabla de entrada "_lyr_in"
                EXECUTE format('
                    UPDATE
                        %s a
                    SET
                        id_a = (	
                            SELECT 
                              b.id_a
                            FROM
                              %s b
                            WHERE
                                b.sicob_id = a.id_a
                            LIMIT 1
                        ),
                        source_a = ''%s''
                ',_out->>'lyr_over',a, (_opt->>'lyr_in')::text);
            END IF;
            sql := format('
                    SELECT 
                    a.id_a,
                    a.source_a,
                    a.id_b,
                    a.source_b,            
                    a.predio,
                    a.propietario,
                    a.the_geom
                    FROM
                        %s a
                    WHERE a.id_b IS NOT NULL         
                    ', (_out->>'lyr_over')::text);
        	IF createdResult THEN
				EXECUTE 'INSERT INTO processed.' || tbl_name || '_pred(id_a, source_a, id_b, source_b, predio, propietario,the_geom) ' || sql;
			ELSE
            	EXECUTE 'DROP TABLE IF EXISTS processed.' || tbl_name || '_pred'; 
            	EXECUTE 'CREATE TABLE processed.' || tbl_name || '_pred' || ' AS ' || sql;
                createdResult := TRUE;
        	END IF;
            IF COALESCE( (_out->>'features_diff_cnt')::int,0) > 0 THEN --> si existen poligonos NO encontrados.
            	a := (_out->>'lyr_over')::text;
                _condition := 'id_b IS NULL';
            ELSE
            	a := '';
            END IF;                        	
        END IF;
	END IF;

----------
--FASE 2 :
----------

-- Se realiza la busqueda en la capa de "predios_pop" vigentes.
_lyr_pred := 'coberturas.predios_pop';

	IF a <> '' THEN --> Si existen poligonos para procesar.
		_out := sicob_overlap(('{"a":"' || a || '","condition_a":"' || _condition || '","b":"' || _lyr_pred || '","subfix":"_pop","tolerance":"' || _tolerance || '","add_diff":true,"temp":true}')::json);
        IF COALESCE( (_out->>'features_inters_cnt')::int,0) > 0 THEN --> Si se han encontrado predios.
        	IF a <> (_opt->>'lyr_in')::text THEN --> Actualizar referencia de la tabla resultado.
                -->Cambiando la referencia de "a" hacia la tabla de entrada "_lyr_in"
                EXECUTE format('
                    UPDATE
                        %s a
                    SET
                        id_a = (	
                            SELECT 
                              b.id_a
                            FROM
                              %s b
                            WHERE
                                b.sicob_id = a.id_a
                            LIMIT 1
                        ),
                        source_a = ''%s''
                ',_out->>'lyr_over',a, (_opt->>'lyr_in')::text);
            END IF;
            sql := format('
                    SELECT 
                    a.id_a,
                    a.source_a,
                    a.id_b,
                    a.source_b,            
                    CAST(a.nom_pre AS text) as predio,
                	CAST(a.nom_pro AS text) as propietario,
                    a.the_geom
                    FROM
                        %s a
                    WHERE a.id_b IS NOT NULL         
                    ', (_out->>'lyr_over')::text);
        	IF createdResult THEN
				EXECUTE 'INSERT INTO processed.' || tbl_name || '_pred(id_a, source_a, id_b, source_b, predio, propietario,the_geom) ' || sql;
			ELSE
            	EXECUTE 'DROP TABLE IF EXISTS processed.' || tbl_name || '_pred'; 
            	EXECUTE 'CREATE TABLE processed.' || tbl_name || '_pred' || ' AS ' || sql;
                createdResult := TRUE;
        	END IF;
            IF COALESCE( (_out->>'features_diff_cnt')::int,0) > 0 THEN --> si existen poligonos NO encontrados.
            	a := (_out->>'lyr_over')::text;
                _condition := 'id_b IS NULL';
            ELSE
            	a := '';
            END IF;                  
        END IF;
	END IF;

----------
--FASE 3 :
----------
-- Se realiza la busqueda en la capa de "predios_proceso_geosicob_geo_201607"

_lyr_pred := 'coberturas.predios_proceso_geosicob_geo_201607';

	IF a <> '' THEN --> Si existen poligonos para procesar.
		_out := sicob_overlap(('{"a":"' || a || '","condition_a":"' || _condition || '","b":"' || _lyr_pred || '","subfix":"_proc","tolerance":"' || _tolerance || '","add_diff":true,"temp":true, "min_sup":"100"}')::json);
        IF COALESCE( (_out->>'features_inters_cnt')::int,0) > 0 THEN --> Si se han encontrado predios.
        	IF a <> (_opt->>'lyr_in')::text THEN --> Actualizar referencia de la tabla resultado.
                -->Cambiando la referencia de "a" hacia la tabla de entrada "_lyr_in"
                EXECUTE format('
                    UPDATE
                        %s a
                    SET
                        id_a = (	
                            SELECT 
                              b.id_a
                            FROM
                              %s b
                            WHERE
                                b.sicob_id = a.id_a
                            LIMIT 1
                        ),
                        source_a = ''%s''
                ',_out->>'lyr_over',a, (_opt->>'lyr_in')::text);
            END IF;
            sql := format('
                    SELECT 
                    a.id_a,
                    a.source_a,
                    a.id_b,
                    a.source_b,            
                    CAST(a.nompred AS text) as predio,
                	CAST(a.beneficiar AS text) as propietario,
                    a.the_geom
                    FROM
                        %s a
         
                    ', (_out->>'lyr_over')::text);
        	IF sicob_exist_table('processed.' || tbl_name || '_pred') THEN
				EXECUTE 'INSERT INTO processed.' || tbl_name || '_pred(id_a, source_a, id_b, source_b, predio, propietario,the_geom) ' || sql;
			ELSE
            	EXECUTE 'DROP TABLE IF EXISTS processed.' || tbl_name || '_pred'; 
            	EXECUTE 'CREATE TABLE processed.' || tbl_name || '_pred' || ' AS ' || sql;
        	END IF;
            IF COALESCE( (_out->>'features_diff_cnt')::int,0) > 0 THEN --> si existen poligonos NO encontrados.
            	a := (_out->>'lyr_over')::text;
                _condition := 'id_b IS NULL';
            ELSE
            	a := '';
            END IF;                  
        END IF;
	END IF;

	RETURN _out;



	_out := sicob_overlap(('{"a":"' || (_opt->>'lyr_in')::text || '","condition_a":"' || COALESCE( (_opt->>'condition')::text , 'TRUE') || '","b":"coberturas.predios_titulados","subfix":"_pred","tolerance":"5.3","add_diff":true,"schema":"temp"}')::json);

    IF COALESCE( (_out->>'features_inters_cnt')::int,0) > 0 THEN
	--AGREGANDO LA RESOLUCION DEL POP A AQUELLOS POLIGONOS QUE SE ENCONTRARON EN LA CAPA DE TITULADOS DEL INRA

         _tmp := sicob_overlap(('{"a":"' || (_out->>'lyr_over')::text || '","condition_a":"titulo IS  NOT NULL","b":"coberturas.predios_pop","subfix":"_pop","tolerance":"5.6","add_diff":false, "temp": true}')::json);

        IF COALESCE( (_tmp->>'features_inters_cnt')::int,0) > 0 THEN --> Si se han encontrado POP.
            sql := '
            UPDATE ' || (_out->>'lyr_over')::text || ' a
            SET resol_pop = b.res_adm, fec_resol_pop = fec_res
            FROM ' || (_tmp->>'lyr_over')::text || ' b
            WHERE
                b.id_a = a.sicob_id
            ';
            RAISE DEBUG 'Running %', sql;
            EXECUTE sql;            
        END IF;

		_tolerance := '0'; --> Si se encontraron predios en la capa del INRA, ya no se deben mover los bordes de los polígono restantes. (TODO: mover solo los bordes no adyacentes a la capa del INRA)
	ELSE
    	_tolerance := '5.3';      
    END IF;
    
    IF COALESCE( (_out->>'features_diff_cnt')::int,0) > 0 THEN --> Si existen poligono cuyos predios NO han sido ubicados en la capa de predios titulados del INRA.
    	---------
    	--FASE 2:
        ---------
        -- Se realiza la busqueda en la capa de "predios_pop" vigentes
        _tmp := sicob_overlap(('{"a":"' || (_out->>'lyr_over')::text || '","condition_a":"titulo IS NULL","b":"coberturas.predios_pop","subfix":"_pop","tolerance":"' || _tolerance || '","add_diff":true, "temp":true}')::json);
	
		IF COALESCE( (_tmp->>'features_inters_cnt')::int,0) > 0 THEN --> Si se han encontrado predios.
			--Agregando los predios encontrados a la capa resultante.
            sql := '
                SELECT 
				a.id_a,
                a.source_a,
                b.id_b,
                b.source_b,
                CAST(b.nom_pre AS text) as predio,
                CAST(b.nom_pro AS text) as propietario,
                res_adm AS resol_pop,
                fec_res AS fec_resol_pop,
                b.the_geom
				FROM
					' || (_out->>'lyr_over')::text || ' a
				INNER JOIN ' || (_tmp->>'lyr_over')::text || ' b ON (a.sicob_id = b.id_a)              
                ';  
                RAISE DEBUG 'Running %', sql;
                EXECUTE 'INSERT INTO ' || (_out->>'lyr_over')::text || '(id_a, source_a, id_b, source_b, predio, propietario, resol_pop, fec_resol_pop, the_geom) ' || sql;
                      
            --Eliminando los poligonos actualizados
            sql := 'DELETE FROM ' || (_out->>'lyr_over')::text || ' 
            	WHERE id_b IS NULL AND CAST(source_b AS text) = ''coberturas.predios_titulados''';
			RAISE DEBUG 'Running %', sql;
			EXECUTE sql;        
        END IF;
     
		IF COALESCE( (_tmp->>'features_diff_cnt')::int,0) > 0 THEN --> Si existen poligono cuyos predios NO han sido ubicados.
    	---------
    	--FASE 3:
        ---------
        -- Se realiza la busqueda en la capa de "predios_proceso_geosicob_geo_201607"
        	_tmp := sicob_overlap(('{"a":"' || (_out->>'lyr_over')::text || '","condition_a":"id_b IS NULL","b":"coberturas.predios_proceso_geosicob_geo_201607","subfix":"_proc","tolerance":"' || _tolerance || '","add_diff":true, "temp":true, "min_sup":"100"}')::json);

			IF COALESCE( (_tmp->>'features_inters_cnt')::int,0) > 0 THEN --> Si se han encontrado predios.
				--Agregando los predios encontrados a la capa resultante.
            	sql := '
                	SELECT 
						a.id_a,
                		a.source_a,
                		b.id_b,
                		b.source_b,
                		CAST(b.nompred AS text) as predio,
                		CAST(b.beneficiar AS text) as propietario,
                		b.the_geom
					FROM
						' || (_out->>'lyr_over')::text || ' a
					INNER JOIN ' || (_tmp->>'lyr_over')::text || ' b ON (a.sicob_id = b.id_a)              
                ';  
                RAISE DEBUG 'Running %', sql;
                EXECUTE 'INSERT INTO ' || (_out->>'lyr_over')::text || '(id_a, source_a, id_b, source_b, predio, propietario,the_geom) ' || sql;                    

            	--Eliminando los poligonos actualizados
            	sql := 'DELETE FROM ' || (_out->>'lyr_over')::text || ' 
            		WHERE id_b IS NULL AND CAST(source_b AS text) IN (''coberturas.predios_pop'', ''coberturas.predios_titulados'' )';
				RAISE DEBUG 'Running %', sql;
				EXECUTE sql;
            END IF;
		END IF;             
    END IF;
    
    --AGREGANDO LA GEOINFORMACION
	PERFORM sicob_add_geoinfo_column((_out->>'lyr_over')::text);
    PERFORM sicob_update_geoinfo_column((_out->>'lyr_over')::text);
    IF _min_sup > 0 THEN
    	sql := 'DELETE FROM ' || (_out->>'lyr_over')::text || 
        ' WHERE sicob_sup < ' || _min_sup::text;
        RAISE DEBUG 'Running %', sql;
        EXECUTE sql;
    END IF;
     
	-- Actualizando la cantidad total de poligonos encontrados
    sql := 'SELECT count(*) FROM ' || (_out->>'lyr_over')::text ||
    ' WHERE id_b IS NOT NULL';
    EXECUTE sql INTO _row_cnt;
    _out := _out::jsonb || jsonb_build_object('features_inters_cnt', _row_cnt );         
	-- Actualizando la cantidad total de poligonos NO encontrados
    sql := 'SELECT count(*) FROM ' || (_out->>'lyr_over')::text ||
    ' WHERE id_b IS NULL';
    EXECUTE sql INTO _row_cnt;
    _out := _out::jsonb || jsonb_build_object('features_diff_cnt', _row_cnt); 

	--COMBINANDO LA INFORMACION DE PREDIOS ENCONTRADOS CON LOS CAMPOS DE LA TABLA DE ENTRADA. 
    tbl_name := (_opt->>'lyr_in')::text;
	SELECT (sicob_split_table_name(tbl_name)).table_name INTO tbl_name ;
    sql := 'DROP TABLE IF EXISTS processed.' || tbl_name || '_pred';
    RAISE DEBUG 'Running %', sql;
    EXECUTE sql;

    sql := 'CREATE TABLE processed.' || tbl_name || '_pred AS ' ||     
    'SELECT ' || 
		'r.sicob_id,' ||
  		'r.predio,' ||
  		'r.propietario,' ||
		'r.titulo,' ||
		'r.fecha_titulo,' ||
  		'r.sup_predio,' ||
  		'r.tipo_propiedad,' ||
  		'r.resol_pop,' ||
  		'r.fec_resol_pop,' ||
        'r.source_b as source_predio,' ||
        'r.id_b as id_predio,' || 
    	sicob_no_geo_column(
        	(_opt->>'lyr_in') ,
            '{sicob_id, predio, propietario, titulo, fecha_titulo, sup_predio, tipo_propiedad, resol_pop, fec_resol_pop, sicob_sup, sicob_utm, id_predio, source_predio}',
        	--> ('{' || sicob_no_geo_column((_opt->>'lyr_over')::text ,'{}',' '::text) || '}')::text[],
            'a.'
        ) || 
        ',r.sicob_sup,r.sicob_utm, r.the_geom, r.the_geom_webmercator' ||
    	' FROM ' || (_out->>'lyr_over')::text || ' r ' ||
  		'INNER JOIN ' || (_opt->>'lyr_in')::text || ' a ON (r.id_a = a.sicob_id) ' ||
        'WHERE r.predio IS NOT NULL ' || --> Filtrando en la capa resultado solamente los poligonos que tienen predio.
        'ORDER BY r.sicob_id';

    RAISE DEBUG 'Running %', sql;
    EXECUTE sql; 

    sql := 'CREATE INDEX ' || tbl_name || '_pred_geomidx
	ON processed.' || tbl_name || '_pred 
	USING GIST (the_geom) ';
	RAISE DEBUG 'Running %', sql;
	EXECUTE sql;

    PERFORM sicob_create_id_column('processed.' || tbl_name || '_pred','sicob_id');
    _out := _out::jsonb || jsonb_build_object('lyr_over', 'processed.' || tbl_name || '_pred'); 
            
	--ADICIONANDO RESULTADO EN FORMATO GEOJSON
    IF COALESCE((_opt->>'geojson')::boolean,FALSE) = TRUE THEN
    	EXECUTE 'SELECT sicob_to_geojson(''{"lyr_in":"' ||  (_out->>'lyr_over')::text || '"}'') AS geojson' INTO _geojson;
        _out := _out::jsonb || jsonb_build_object('lyr_geojson', _geojson); 
	END IF;
 
	--CREANDO LAS CAPAS TEMPORALES DE LOS PREDIOS ENCONTRADOS
    IF COALESCE((_out->>'features_inters_cnt')::integer,0) > 0 THEN
    	sql := '
          SELECT row_to_json(u)
          FROM
          (
              SELECT array_to_json(array_agg(t)) as predios, count(t.*) as total_predios FROM 
              (
                  SELECT  
                    a.predio,
                    a.propietario,
                    a.titulo,
                    a.fecha_titulo,
                    a.resol_pop,
                    a.fec_resol_pop,
                    SUM(a.sicob_sup) AS sicob_sup
                  FROM
                    ' || (_out->>'lyr_over')::text || ' a
                  WHERE
                    predio IS NOT NULL
                  GROUP BY
                    a.predio,
                    a.propietario,
                    a.titulo,
                    a.fecha_titulo,
                    a.resol_pop,
                    a.fec_resol_pop
                  ORDER BY a.predio
              ) t
          )u
        ';   
		RAISE DEBUG 'Running %', sql;
		EXECUTE sql INTO _tmp;

        _out := _out::jsonb || _tmp::jsonb;       
		sql := 'DROP TABLE IF EXISTS temp.' || tbl_name || '_titulados; ' ||
		'CREATE TABLE temp.' || tbl_name || '_titulados AS ' ||
        'SELECT ' || sicob_no_geo_column('coberturas.predios_titulados' ,'{resol_pop,fec_resol_pop}',' ') || ', the_geom ' ||
        'FROM coberturas.predios_titulados 
		WHERE 
			sicob_id IN (
				SELECT DISTINCT id_predio FROM ' || (_out->>'lyr_over')::text ||
			'	WHERE id_predio IS NOT NULL AND source_predio = ''coberturas.predios_titulados''
			)';
        RAISE DEBUG 'Running %', sql;
    	EXECUTE sql;

        GET DIAGNOSTICS _row_cnt = ROW_COUNT; 

        IF _row_cnt > 0 THEN
        	_out := _out::jsonb || jsonb_build_object('lyr_titulados', 'temp.' || tbl_name || '_titulados'); 
        END IF;
      
		sql := 'DROP TABLE IF EXISTS temp.' || tbl_name || '_pred_varios; ' ||
		'CREATE TABLE temp.' || tbl_name || '_pred_varios AS ' ||
        'SELECT * FROM coberturas.predios_proceso_geosicob_geo_201607 
		WHERE 
			sicob_id IN (
				SELECT DISTINCT id_predio FROM ' || (_out->>'lyr_over')::text ||
			'	WHERE id_predio IS NOT NULL AND source_predio = ''coberturas.predios_proceso_geosicob_geo_201607''
			)';

        RAISE DEBUG 'Running %', sql;
    	EXECUTE sql; 
        GET DIAGNOSTICS _row_cnt = ROW_COUNT;        
        IF _row_cnt > 0 THEN
        	_out := _out::jsonb || jsonb_build_object('lyr_pred_varios', 'temp.' || tbl_name || '_pred_varios'); 
        END IF;        

		sql := 'DROP TABLE IF EXISTS temp.' || tbl_name || '_pred_pop; ' ||
		'CREATE TABLE temp.' || tbl_name || '_pred_pop AS ' ||
        'SELECT * FROM coberturas.predios_pop
		WHERE 
			res_adm IN (
				SELECT DISTINCT resol_pop FROM ' || (_out->>'lyr_over')::text ||
			'	WHERE resol_pop IS NOT NULL 
			)';
        RAISE DEBUG 'Running %', sql;
    	EXECUTE sql; 
        GET DIAGNOSTICS _row_cnt = ROW_COUNT;        

        IF _row_cnt > 0 THEN
        	_out := _out::jsonb || jsonb_build_object('lyr_pred_pop', 'temp.' || tbl_name || '_pred_pop'); 
        END IF;
	END IF;     	   

	RETURN _out;   
	
    EXCEPTION
		WHEN others THEN
			RAISE EXCEPTION 'geoSICOB (sicob_obtener_predio): _opt: % >> %, (%)', _opt::text, SQLERRM, SQLSTATE;

END;
$function$
 