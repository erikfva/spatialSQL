CREATE OR REPLACE FUNCTION public.sicob_obtener_predio3(_opt json)
 RETURNS json
 LANGUAGE plpgsql
AS $function$
DECLARE 
 sql text;
 _out json := '{}';
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
 lyrs_predio json[];
 lyr_predio json;
 tmp json;
 find_cnt integer;
BEGIN

--VARIABLES DE ENTRADA:
--	lyr_in : capa con los poligonos que deben ser ubicados en sus predios correspondientes
--	condition (opcional):  Filtro para los datos de la capa "lyr_in". Si no se especifica, se toman todos los registros.
--> lyr_pred : capa con predios de referencia.
--> fldpredio : campo que contiene el nombre del predio en la capa de referencia.
--> fldpropietario : campo que contiene el nombre del propietario en la capa de referncia. 
--> geojson (opcional true/false): Devuelve el resultado en formato geojson. Por defecto es false.
--> min_sup (opcional): Superficie minima (en hectareas) permitida para los poligonos de la capa resultado por defecto es igual a 0.002 que fue la minima encontrada en los PDM.
--> tolerance (opcional): Distancia máxima (en metros) para el autoajuste automático de los bordes de "a" hacia los bordes de "b" (snapping). Si no se especifica, no se realiza autoajuste.

--VARIABLES DE SALIDA:
--> lyr_over: Nombre de la capa resultante, este es igual al nombre de la capa de entrada más el subfijo “_ppred”. Esta capa siempre será creada en el esquema “temp” de la BD. 
--> features_inters_cnt: Cantidad de polígonos que SI se han encontrado sus predios.
--> features_diff_cnt: Cantidad de polígonos de los cuales NO se ha encontrado predio.
--> lyr_geojson : Si se ha indicado el valor geojson de entrada como "true", devuelve la capa resultado en formato geojson.
--> lyr_pred: Nombre de la capa que contiene los predios encontrados.
--> predios : Array json con el detalle de los predios encontrados.
--> total_predios: Cantidad de predios encontrados.
 
--_opt := '{"lyr_in":"processed.f20161118dgfaceb9e93b41b_nsi"}'::json;
--_opt := '{"lyr_in":"coberturas.b0603206_pdm_a052016","condition":"objectid_1 = 35884"}';
--_opt := '{"lyr_in":"coberturas.b0603206_pdm_a052016","condition":"objectid_1 = 11426"}';
--_opt := '{"lyr_in":"coberturas.pdm","condition":"res_adm = ''RS-OLSC-PDM-129-2000''"}';
--_opt := '{"lyr_in":"uploads.f20170704gcfebdac5d7c097","lyr_pred":"uploads.f20170705ecgdbfafbc4c20f"}'::json;

_min_sup := COALESCE((_opt->>'min_sup')::real, 0.002); --> Esta superficie es la menor existente en la BD
_tolerance := COALESCE((_opt->>'tolerance')::text, '5.3');
tbl_name := (_opt->>'lyr_in')::text;
	SELECT (sicob_split_table_name(tbl_name)).table_name INTO tbl_name ;
_condition := COALESCE( (_opt->>'condition')::text , 'TRUE');

lyrs_predio := ARRAY[
    					format('{"lyr":"%s","fldpredio":"%s","fldpropietario":"%s","subfix":"_tit"}',
                        	'coberturas.predios_titulados', 
                            'predio',
                            'propietario')
                        ,                        
    					format('{"lyr":"%s","fldpredio":"%s","fldpropietario":"%s","subfix":"_pop","tolerance":"0"}',
                        	'coberturas.predios_pop', 
                            'nom_pre',
                            'nom_pro')
                        ,
    					format('{"lyr":"%s","fldpredio":"%s","fldpropietario":"%s","subfix":"_proc","min_sup":"100","tolerance":"0"}',
                        	'coberturas.predios_proceso_geosicob_geo_201607', 
                            'nompred',
                            'beneficiar')
					];
IF COALESCE( (_opt->>'lyr_pred')::text,'') <> '' THEN --> Si se indica cobertura de referencia.
   	IF NOT sicob_exist_column(_opt->>'lyr_pred', COALESCE((_opt->>'fldpredio')::text, 'predio')) THEN
       	_fldpredio := '''COLUMNA NO ENCONTRADA''';
   	END IF;
   	IF NOT sicob_exist_column(_opt->>'lyr_pred', COALESCE((_opt->>'fldpropietario')::text, 'propietario')) THEN
      	_fldpropietario := '''COLUMNA NO ENCONTRADA''';
   	END IF;
	lyr_predio := format('{"lyr":"%s","fldpredio":"%s","fldpropietario":"%s","subfix":"_ref"}',
                        	(_opt->>'lyr_pred')::text, 
                            _fldpredio,
                            _fldpropietario
                  );
	SELECT array_prepend(lyr_predio::json , lyrs_predio::json[]) INTO lyrs_predio;
END IF;

find_cnt := 0;                  
a := (_opt->>'lyr_in')::text;  
--> ANALIZANDO CADA COBERTURA DE PREDIOS 
FOR i IN array_lower(lyrs_predio, 1)..array_upper(lyrs_predio, 1) LOOP        
    lyr_predio := lyrs_predio[i]; --> Obteniendo los datos de procesamiento de la cobertura de predios.
	IF a <> '' AND lyr_predio->>'lyr' <> '' THEN --> Si existen poligonos para localizar.
    	_out := sicob_overlap(('{"a":"' || a || 
        			'","condition_a":"' || _condition || 
                    '","b":"' || (lyr_predio->>'lyr')::text || 
                    '","subfix":"' || (lyr_predio->>'subfix')::text || 
                    '","tolerance":"' || COALESCE((lyr_predio->>'tolerance')::text, _tolerance ) || 
                    '","add_diff":true,"temp" : true, ' || 
                    '"min_sup":"' || COALESCE((lyr_predio->>'min_sup')::text, '0') ||
                    '"}')::json); 
        IF COALESCE( (_out->>'features_inters_cnt')::int,0) > 0 THEN  --> Si se han localizado predios.
        
        	find_cnt := find_cnt +  (_out->>'features_inters_cnt')::int;
       
        	IF a <> (_opt->>'lyr_in')::text THEN --> Actualizar referencia de la tabla resultado.
                -->Cambiando la referencia "id_a" de "a" hacia la tabla de entrada "_lyr_in"
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
            
        	_fldpredio := (lyr_predio->>'fldpredio')::text;
        	_fldpropietario := (lyr_predio->>'fldpropietario')::text;
            
            sql := format('
                    SELECT 
                    id_a,
                    source_a,
                    id_b,
                    source_b,            
                    CAST(%s AS text) as predio,
                    CAST(%s AS text) as propietario, ' ||
                    CASE (lyr_predio->>'subfix')::text WHEN '_tit' 
                    	THEN 'titulo' 
                        ELSE 'CAST(NULL AS text) as titulo' 
                    END || ', ' ||
                    CASE (lyr_predio->>'subfix')::text WHEN '_tit' 
                    	THEN 'fecha_titulo' 
                        ELSE 'CAST(NULL AS timestamp with time zone) AS fecha_titulo' 
                    END || ', ' || 
                    CASE (lyr_predio->>'subfix')::text WHEN '_tit' 
                    	THEN 'tipo_propiedad' 
                        ELSE 'CAST(NULL AS text) AS tipo_propiedad' 
                    END || ', ' ||
                    CASE (lyr_predio->>'subfix')::text 
                    	WHEN '_ref'	THEN 'sicob_sup AS sup_predio'
                        WHEN '_tit'	THEN 'sup_predio'
                        WHEN '_pop' THEN 'sup_pre AS sup_predio' 
                        WHEN '_proc' THEN 'sicob_sup AS sup_predio' 
                        ELSE 'CAST(NULL AS float) AS sup_predio' 
                    END || ', ' ||
                    CASE (lyr_predio->>'subfix')::text WHEN '_pop' 
                    	THEN 'res_adm AS resol_pop' 
                        ELSE 'CAST(NULL AS text) AS resol_pop' 
                    END || ', ' ||
                    CASE (lyr_predio->>'subfix')::text WHEN '_pop' 
                    	THEN 'fec_res AS fec_resol_pop' 
                        ELSE 'CAST(NULL  AS timestamp with time zone) AS fec_resol_pop' 
                    END || ', ' ||
                    'the_geom
                    FROM
                        %s
                    WHERE id_b IS NOT NULL         
                    ',_fldpredio,_fldpropietario, (_out->>'lyr_over')::text);
        	IF createdResult THEN
				EXECUTE 'INSERT INTO ' || tbl_name || '_ppred ' || sql;
            ELSE
                EXECUTE 'CREATE TEMPORARY TABLE ' || tbl_name || '_ppred' || ' ON COMMIT DROP AS ' || sql;
        	END IF;
--> CREANDO LA COBERTURA TEMPORAL DE PREDIOS
            sql := format('
                    SELECT 
                    sicob_id as id_predio,
                    CAST(''%s'' AS text) AS source_predio,           
                    CAST(%s AS text) as predio,
                    CAST(%s AS text) as propietario, ' ||
                    CASE (lyr_predio->>'subfix')::text WHEN '_tit' 
                    	THEN 'titulo' 
                        ELSE 'CAST(NULL AS text) as titulo' 
                    END || ', ' ||
                    CASE (lyr_predio->>'subfix')::text WHEN '_tit' 
                    	THEN 'fecha_titulo' 
                        ELSE 'CAST(NULL AS timestamp with time zone) AS fecha_titulo' 
                    END || ', ' || 
                    CASE (lyr_predio->>'subfix')::text WHEN '_tit' 
                    	THEN 'tipo_propiedad' 
                        ELSE 'CAST(NULL AS text) AS tipo_propiedad' 
                    END || ', ' ||
                    CASE (lyr_predio->>'subfix')::text 
                    	WHEN '_ref'	THEN 'sicob_sup AS sup_predio'
                        WHEN '_tit'	THEN 'sup_predio'
                        WHEN '_pop' THEN 'sup_pre AS sup_predio' 
                        WHEN '_proc' THEN 'sicob_sup AS sup_predio' 
                        ELSE 'CAST(NULL AS float) AS sup_predio' 
                    END || ', ' ||
                    CASE (lyr_predio->>'subfix')::text WHEN '_pop' 
                    	THEN 'res_adm AS resol_pop' 
                        ELSE 'CAST(NULL AS text) AS resol_pop' 
                    END || ', ' ||
                    CASE (lyr_predio->>'subfix')::text WHEN '_pop' 
                    	THEN 'fec_res AS fec_resol_pop' 
                        ELSE 'CAST(NULL  AS timestamp with time zone) AS fec_resol_pop' 
                    END || ', ' ||
                    'ST_Multi(the_geom) AS the_geom
                    FROM
                        %s
                    WHERE
                    	sicob_id IN (
                        	SELECT id_b FROM %s WHERE id_b IS NOT NULL
                        )        
                    ',(lyr_predio->>'lyr')::text,_fldpredio,_fldpropietario,(lyr_predio->>'lyr')::text, (_out->>'lyr_over')::text);
        	IF createdResult THEN
				EXECUTE 'INSERT INTO temp.' || tbl_name || '_pred ' || sql;
            ELSE
            	EXECUTE 'DROP TABLE IF EXISTS temp.' || tbl_name || '_pred'; 
                EXECUTE 'CREATE TABLE temp.' || tbl_name || '_pred' || ' AS ' || sql;
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
END LOOP;

IF find_cnt > 0 THEN --> Si se han encontrado poligonos.

	--> CREANDO LA COBERTURA RESULTANTE.

    sql := 'SELECT  
		CAST(row_number() OVER () AS integer) AS sicob_id,
  		r.predio,
  		r.propietario,
		r.titulo,
		r.fecha_titulo,
  		r.sup_predio,
  		r.tipo_propiedad,
  		r.resol_pop,
  		r.fec_resol_pop,
        r.source_b as source_predio,
        r.id_b as id_predio,' || 
    	sicob_no_geo_column(
        	(_opt->>'lyr_in') ,
            '{sicob_id, predio, propietario, titulo, fecha_titulo, sup_predio, tipo_propiedad, resol_pop, fec_resol_pop, sicob_sup, sicob_utm, id_predio, source_predio}',
        	--> ('{' || sicob_no_geo_column((_opt->>'lyr_over')::text ,'{}',' '::text) || '}')::text[],
            'a.'
        ) || 
        ',r.the_geom
    	 FROM ' || tbl_name || '_ppred' || ' r 
  		 INNER JOIN ' || (_opt->>'lyr_in')::text || ' a ON (r.id_a = a.sicob_id) ' ||
--        'WHERE r.predio IS NOT NULL ' || --> Filtrando en la capa resultado solamente los poligonos que tienen predio.
        'ORDER BY a.sicob_id';
	a := 'temp.' || tbl_name || '_ppred'; --> Nombre de la capa resultante de intersectar los poligonos de entrada con los predios.
    EXECUTE 'CREATE TEMPORARY TABLE ' || tbl_name || '_ppred1' || ' ON COMMIT DROP AS ' || sql;
--> Complementando información de ubicación politico-administrativo.
    sql := 'SELECT t.*,u.nom_dep, u.nom_prov, u.nom_mun, u.cod_mun 
    	FROM
        	' || tbl_name || '_ppred1' || ' t LEFT OUTER JOIN
			(SELECT * FROM sicob_ubication(''' || tbl_name || '_ppred1' || ''') ) u
			ON (u.sicob_id = t.sicob_id)
		';  
	EXECUTE 'DROP TABLE IF EXISTS ' || a; 
    EXECUTE 'CREATE TABLE ' || a || ' AS ' || sql;
	GET DIAGNOSTICS _row_cnt = ROW_COUNT;
--> CREANDO INDICE GEOGRAFICO
    sql := 'CREATE INDEX ' || tbl_name || '_ppred_geomidx
	ON ' || a || '  
	USING GIST (the_geom) ';
	RAISE DEBUG 'Running %', sql;
	EXECUTE sql;           
--> AGREGANDO INFORMACION DE POP A LOS POLIGONOS TITULADOS Y DE REFERENCIA
	_condition := 'source_predio = ''coberturas.predios_titulados''';    
	IF COALESCE( (_opt->>'lyr_pred')::text,'') <> '' THEN --> Si se indica cobertura de referencia.
    	_condition := _condition || ' OR source_predio = ''' || (_opt->>'lyr_pred')::text || '''';
    END IF;
    tmp := sicob_overlap(('{"a":"' || a || '","condition_a":"' || _condition || '","b":"coberturas.predios_pop","subfix":"_pop","tolerance":"0","add_diff":false, "temp": true}')::json);
    IF COALESCE( (tmp->>'features_inters_cnt')::int,0) > 0 THEN --> Si se han encontrado POP.
        sql := '
            UPDATE ' || a || ' a
            SET resol_pop = b.res_adm, fec_resol_pop = fec_res
            FROM ' || (tmp->>'lyr_over')::text || ' b
            WHERE
            b.id_a = a.sicob_id
        ';
        RAISE DEBUG 'Running %', sql;
        EXECUTE sql;            
    END IF;
    
--> AGREGANDO INFORMACION DE TITULACION A LOS POLIGONOS DE REFERENCIA
	IF COALESCE( (_opt->>'lyr_pred')::text,'') <> '' THEN --> Si se indica cobertura de referencia.
    	_condition := 'source_predio = ''' || (_opt->>'lyr_pred')::text || '''';
        tmp := sicob_overlap(('{"a":"' || a || '","condition_a":"' || _condition || '","b":"coberturas.predios_titulados","subfix":"_tit","tolerance":"0","add_diff":false, "temp": true}')::json);
        IF COALESCE( (tmp->>'features_inters_cnt')::int,0) > 0 THEN --> Si se han encontrado POP.
            sql := '
                UPDATE ' || a || ' a
                SET titulo = b.titulo, fecha_titulo = b.fecha_titulo, sup_predio = b.sup_predio, tipo_propiedad = b.tipo_propiedad
                FROM ' || (tmp->>'lyr_over')::text || ' b
                WHERE
                b.id_a = a.sicob_id
            ';
            RAISE DEBUG 'Running %', sql;
            EXECUTE sql;            
        END IF;
    END IF;

    
    --AGREGANDO LA GEOINFORMACION
	PERFORM sicob_add_geoinfo_column(a);
    PERFORM sicob_update_geoinfo_column(a);

    _out := _out::jsonb || ('{"features_inters_cnt":"' || find_cnt::text || 
    '", "features_diff_cnt":"' || (_row_cnt - find_cnt)::text || 
    '", "lyr_over":"' || a || '", "lyr_pred":"temp.' || tbl_name || '_pred"}')::jsonb;
    
	-->CREANDO EL DETALLE DE LOS PREDIOS ENCONTRADOS
    sql := '
      SELECT row_to_json(u)
      FROM
      (
          SELECT array_to_json(array_agg(t)) as predios, count(t.*) as total_predios FROM 
          (
              SELECT
                a.id_predio,  
                a.predio,
                a.source_predio,
                a.propietario,
                a.titulo,
                a.fecha_titulo,
                a.resol_pop,
                a.fec_resol_pop,
                a.sup_predio
              FROM
                temp.' || tbl_name || '_pred a
              ORDER BY a.source_predio, a.id_predio
          ) t
      )u
    ';   
    RAISE DEBUG 'Running %', sql;
    EXECUTE sql INTO tmp;
    _out := tmp::jsonb || _out::jsonb; 
    
	--ADICIONANDO RESULTADO EN FORMATO GEOJSON
    IF COALESCE((_opt->>'geojson')::boolean,FALSE) = TRUE THEN
    	EXECUTE 'SELECT sicob_to_geojson(''{"lyr_in":"' ||  a || '"}'') AS geojson' INTO _geojson;
        _out := _out::jsonb || jsonb_build_object('lyr_geojson', _geojson::json); 
	END IF;        
  
END IF;

RETURN _out;
	
    EXCEPTION
		WHEN others THEN
			RAISE EXCEPTION 'geoSICOB (sicob_obtener_predio): _opt: % >> %, (%)', _opt::text, SQLERRM, sql;

END;
$function$
 