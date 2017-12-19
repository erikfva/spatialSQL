CREATE OR REPLACE FUNCTION public.sicob_overlap3(_opt json)
 RETURNS json
 LANGUAGE plpgsql
AS $function$
DECLARE 
  a TEXT;
  b TEXT;
  _subfixresult text;
  _schema text;
  _tolerance double precision;
  _sup float;
  _min_sup float;
---------------------------------
 sql text;
 colsname text;
 tbl_nameA text; sch_nameA text;
 tbl_nameB text; sch_nameB text;
 tbl_subfixDiff text; tbl_subfixResult text;
 row_cnt integer;
 __a text; id__a text;
 _out json := '{}';
 -------------------------------
  arrcol  varchar[];
  col varchar;
  s varchar;
  subfixID varchar;
BEGIN

---------------------------
--PARAMETROS DE ENTRADA
---------------------------
--> a : capa que se sobrepondrá.
--> condition_a (opcional): Filtro para los datos de "a". Si no se especifica, se toman todos los registros.
--> b : capa donde se sobrepondrá la capa "a".
--> condition_b (opcional): Filtro para los datos de "b". Si no se especifica, se toman todos los registros.
--> subfix (opcional): texto adicional que se agregará al nombre de "a" para formar el nombre de la capa resultante. Si no se especifica por defecto es "_overlap".
--> schema (opcional): esquema del la BD donde se creará la capa resultante. Si no se especifica se creará en "processed".
--> tolerance (opcional): Distancia máxima (en metros) para el autoajuste automático de los bordes de "a" hacia los bordes de "b" (snapping). Si no se especifica, no se realiza autoajuste. 
--> add_geoinfo (opcional true/false): Agrega o no la información del código de proyección UTM 'sicob_utm', superficie (ha) 'sicob_sup' y la geometría en proyección webmercator 'the_geom_webmercator' para cada polígono. Si no se especifica NO se agrega.
--> add_diff (opcional true/false): Agrega o no los polígono que no se intersectan a la  capa resultado. Se asigna "NULL" a los campos de atributos para esos polígonos. 
--> temp (opcional true/false) : Indica si la capa resultante será temporal mientras dura la transacción. Esto se requiere cuando el resultado es utilizado como capa intermedia en otros procesos dentro de la misma transacción.
--> add_sup_total (opcional true/false): Calcula y devuelve la superficie total sobrepuesta en hectareas.
--> min_sup (opcional): Superficie minima (en hectareas) permitida para los poligonos de la capa resultado, solo se aplica si se el parametro add_geoinfo es TRUE.
--> filter_overlap (opcional true/false) : Indica si se permite o no sobreposicion de poligonos en la capa resultado. Por defecto es TRUE.
---------------------------
--VALORES DEVUELTOS
---------------------------
--> lyr_over : capa resultante de la sobreposición.
--> features_inters_cnt : cantidad de poligonos que se intersectan.
--> features_diff_cnt : cantidad de poligonos que NO se intersectan.
--> sicob_sup_total : superficie total sobrepuesta en hectareas. (si se indicó el parámetro "add_sup_total")
 
/*
a :=  'processed.f20160708agfbdecf223329c_nsi';--'processed.f20160929daebgcf1933cf73_nsi';
b := 'coberturas.predios_titulados';
_tolerance := 6;
*/

/*
_opt := '{"a":"processed.f20160708agfbdecf223329c_nsi_pred","condition_a":"titulo IS NULL and sicob_id =36","b":"coberturas.predios_proceso_geosicob_geo_201607","subfix":"_proc","tolerance":"5.3"}';
*/

/*
_opt := '{"a":"processed.f20160708agfbdecf223329c_nsi_pred","condition_a":"titulo IS NULL","b":"coberturas.predios_proceso_geosicob_geo_201607","subfix":"_proc","tolerance":"5.6"}';
*/

/*
_opt := '{"a":"coberturas.b0603206_pdm_a052016","condition_a":"objectid_1 = 35884","b":"coberturas.predios_titulados","subfix":"_proc","tolerance":"5.6","add_diff":true}';
*/

/*
_opt := '{"a":"processed.f20161118dgfaceb9e93b41b_nsi", "b":"coberturas.predios_titulados","subfix":"_pred","tolerance":"5.2", "add_diff":true}';
*/

/*
_opt := '{"a":"processed.pdm_pred","condition_a":"titulo IS NULL","b":"coberturas.predios_pop","subfix":"_pop","tolerance":"5.3","add_diff":true, "temp":true}';
*/

/*
_opt := '{"a":"processed.f20160829ecadgfb05235623_nsi","b":"coberturas.predios_titulados","subfix":"_pop","tolerance":"5.3","add_diff":true, "schema":"temp"}';
*/
/*
_opt := '{"a":"processed.f20170718fagebdcf580ac83_nsi","b":"coberturas.predios_titulados","subfix":"_tit","tolerance":"5.3","add_diff":true, "schema":"temp"}';
*/

/*
_opt := '{"a":"uploads.f20170727dabcgefcfe22301","b":"coberturas.parcelas_tituladas","subfix":"_tit","tolerance":"0","add_diff":true, "schema":"temp"}';
*/
SELECT nextval('subfixID_seq') INTO subfixID;
a := (_opt->>'a')::TEXT;
b := (_opt->>'b')::TEXT;
_subfixresult := _opt->>'subfix';
_schema := COALESCE(_opt->>'schema','processed');
_tolerance := COALESCE((_opt->>'tolerance')::real, 0);
_min_sup := COALESCE((_opt->>'min_sup')::real, 0);

SELECT * FROM sicob_split_table_name(a::text) INTO sch_nameA, tbl_nameA;
SELECT * FROM sicob_split_table_name(b::text) INTO sch_nameB, tbl_nameB;

  tbl_subfixDiff := '' || tbl_nameA || '_diff';

IF _subfixresult IS NULL OR _subfixresult = '' THEN
	_subfixresult := '_overlap';
END IF;
IF _schema IS NULL OR _schema = '' THEN
	_schema := 'processed';
END IF;
IF COALESCE((_opt->>'temp')::boolean,FALSE) = TRUE THEN
	tbl_subfixResult := tbl_nameA || _subfixResult;
ELSE
	tbl_subfixResult := _schema || '.' || tbl_nameA || _subfixResult;
END IF;

-->__a := 'temp.__a';
	__a := 'temp.__a' || subfixID;
    id__a := COALESCE(sicob_feature_id(a::text), 'sicob_id');
  
IF _tolerance > 0 THEN
    -----------------------------------------
    --AJUSTANDO LOS BORDES DE a HACIA b 
    --EN UNA DISTANCIA APROXIMADA A _tolerance (en metros)
    -----------------------------------------
	-- feature "a" ajustado a los bordes de "b"


	sql := '
SELECT ' ||
    'sicob_id, ' ||
	sicob_no_geo_column(a::text,'{sicob_id}','t.') || ', ' || 
	'CASE WHEN ST_NRings(the_geom) > 1 THEN

    		sicob_update_exteriorring( 
        		sicob_snap_edge(
        			st_exteriorring(the_geom),
                	''{"target":"' || b::text || '","tolerance":"' || _tolerance::text || '"}''
            	), 
            	the_geom, 
            	''{}''
        	)

	ELSE

			sicob_snap_edge(
				st_exteriorring(the_geom),
				''{"target":"' || b::text || '","tolerance":"' || _tolerance::text || '","returnpolygon":true}''
			)

	END
	AS the_geom
FROM 
	(
		SELECT ' ||
        	id__a || ' AS sicob_id, ' ||
            sicob_no_geo_column(a::text,'{sicob_id}','a.') || ', ' ||
			'(ST_Dump(' || sicob_geo_column(a::text,'a.') || ')).geom as the_geom 
		FROM 
			' || a::text || ' a 
		WHERE TRUE ' || COALESCE( 'AND (' ||  (_opt->>'condition_a')::text || ')', '') ||
        ' LIMIT 100 --> EL LIMITE DE POLIGONOS A PROCESAR ES 100 PARA EVITAR PROCESOS LARGOS!!!
	) t ';   
    
    RAISE DEBUG 'Running %', sql;
    --EXECUTE 'CREATE TEMPORARY TABLE ' || __a || ' ON COMMIT DROP AS ' || sql;
    
    EXECUTE 'CREATE UNLOGGED TABLE ' || __a || ' AS ' || sql;
    
        sql := 'DROP INDEX IF EXISTS ' || tbl_nameA || '_snap_geomidx';
        RAISE DEBUG 'Running %', sql;
        EXECUTE sql;
        
        sql := 'CREATE INDEX __a' || subfixID || '_geomidx
        ON ' || __a || ' 
        USING GIST (the_geom) ';
        RAISE DEBUG 'Running %', sql;
        EXECUTE sql;
ELSE
	IF COALESCE((_opt->>'condition_a')::text , '') <> '' THEN
    	sql := 'SELECT * FROM ' ||  a::text || '
		WHERE TRUE ' || COALESCE( 'AND (' ||  (_opt->>'condition_a')::text || ')', '') ;
        RAISE DEBUG 'Running %', sql;
        --EXECUTE 'CREATE TEMPORARY TABLE ' || __a || ' ON COMMIT DROP AS ' || sql;
        
		EXECUTE 'CREATE UNLOGGED TABLE ' || __a || ' AS ' || sql;
	ELSE
    	__a := a::text;
	END IF; 	
END IF;


-----------------------------------------
--CREANDO LA COBERTURA QUE SE INTERSECTA 
-----------------------------------------

sql := 'DROP TABLE IF EXISTS ' || tbl_subfixResult;
RAISE DEBUG 'Running %', sql;
EXECUTE sql;

colsname := sicob_no_geo_column(b::text,'{sicob_id, id_a, source_a, id_b, source_b}','b.');

s := COALESCE((_opt->>'condition_b')::text, '');
IF (s <> '') THEN
	arrcol := string_to_array(colsname, ',') ;
	FOREACH col IN ARRAY arrcol LOOP
    	s := replace(s, col, 'b.' || col);
	END LOOP;
    s := '(' || s || ')';
ELSE
	s := 'TRUE';
END IF;

sql := '
WITH 
a_intersect_b AS (
SELECT 
	row_number() over() AS ___$, ' ||
  'a.' || id__a || ' as id_a, ''' || a::text || ''' as source_a, 
  b.sicob_id as id_b, ''' || b::text || ''' as source_b,
  CASE 
   WHEN ST_CoveredBy(a.the_geom, b.the_geom) 
   THEN a.the_geom 
  ELSE 
    (ST_Dump(sicob_intersection(
    			a.the_geom, 
                b.the_geom,
                ''POLYGON''
             )
     )
    ).geom
  END
   AS the_geom,' ||
  colsname ||
' FROM  ' || __a || ' a,' || b::text || ' b
WHERE ' || s || '
 AND (st_intersects(a.the_geom, b.the_geom))
  AND NOT ST_Touches(a.the_geom, b.the_geom)
),
overlayed AS (
        --filtrando los polignos que se sobreponenen en la misma capa
            SELECT 
            CASE WHEN st_area(b1.the_geom) < st_area(b2.the_geom) THEN
                b1.___$
            ELSE
                b2.___$
            END as id
            FROM
                a_intersect_b b1,
                a_intersect_b b2
            WHERE 
            	' || COALESCE((_opt->>'filter_overlap')::text, 'TRUE') || ' AND
                b1.___$ <> b2.___$ and
                b1.___$ < b2.___$ and
                ST_Intersects(b1.the_geom,b2.the_geom) 
                AND NOT ST_Touches(b1.the_geom, b2.the_geom)
),
spikeclean AS (
	SELECT 
		id_a, CAST(source_a as text) as source_a, id_b, 
        CAST(source_b as text) as source_b, ' || colsname || ', 
    	sicob_spikeremover(the_geom, 0.01) as the_geom
	FROM a_intersect_b b
    WHERE
    	___$ NOT IN (SELECT id FROM overlayed)
)
SELECT * FROM spikeclean
    WHERE st_geometrytype(the_geom) in (''ST_Polygon'')
    AND trunc(st_area(the_geom)*10000000000) > 0';
 
------------------------------------------------
--ALMACENADO LOS POLIGONOS EN LA COBERTURA DE RESULTADOS 
------------------------------------------------
RAISE DEBUG 'Running %', sql;

IF COALESCE((_opt->>'temp')::boolean,FALSE) = TRUE THEN
	EXECUTE 'CREATE TEMPORARY TABLE ' || tbl_subfixResult || ' ON COMMIT DROP AS ' || sql;
ELSE
	EXECUTE 'CREATE TABLE ' || tbl_subfixResult || ' AS (' || sql || ')';
END IF;

GET DIAGNOSTICS row_cnt = ROW_COUNT; -->obteniendo la cantidad de poligonos que se intersectan
_out := _out::jsonb || jsonb_build_object('features_inters_cnt', row_cnt); 

--PERFORM sicob_create_id_column(tbl_subfixResult); <-- mas lento!!!
EXECUTE 'ALTER TABLE ' || tbl_subfixResult || ' ADD sicob_id SERIAL NOT NULL UNIQUE';

--SI EXISTE INTERSECCION
IF row_cnt > 0 THEN
	--ACTUALIZANDO LA INFORMACION DE LA PROYECCION:
	EXECUTE 'ALTER TABLE ' || tbl_subfixResult || ' ALTER COLUMN the_geom TYPE geometry(POLYGON, 4326) USING ST_SetSRID(the_geom, 4326)';

	--CREANDO EL INDICE ESPACIAL
	sql := 'CREATE INDEX ' ||  tbl_nameA || _subfixResult ||'_the_geom_idx ON ' || tbl_subfixResult || '
  USING gist (the_geom public.gist_geometry_ops_2d)';
	RAISE DEBUG 'Running %', sql;
	EXECUTE sql;
END IF;

--AGREGANDO POLIGONOS QUE NO SE INTERSECTAN
IF COALESCE((_opt->>'add_diff')::boolean,FALSE) = TRUE THEN
    
    ------------------------------------------------
    --CREANDO LA COBERTURA QUE !-NO-! SE INTERSECTA 
    ------------------------------------------------

    sql := 'DROP TABLE IF EXISTS ' || tbl_subfixDiff;
    RAISE DEBUG 'Running %', sql;
    EXECUTE sql;

    --SI NO EXISTE INTERSECCION
    IF row_cnt = 0 THEN
        sql := 'SELECT ' ||
		id__a || ' AS id_a, the_geom FROM ' || __a;
    ELSE
	sql := '
	WITH
	borde AS (
		SELECT ' ||
			id__a || ' AS sicob_id,
	 		st_buffer(
            	st_exteriorring(
                	(st_dump(the_geom)).geom
                ) ,
                0.000000001
            ) as the_geom
		FROM
			' || __a || '
	),   
    diff AS ( 
		SELECT a.' || id__a || ' AS sicob_id, 
		(ST_Dump( 
    		COALESCE(
        		ST_Difference(
            		the_geom, 
                	(SELECT ST_Union( b.the_geom ) 
                 		FROM ' || tbl_subfixResult || ' b
					)
				), 
				a.the_geom
			) 
		)).geom as the_geom
        FROM ' || __a || ' a
	),
	cleandiff AS (
		SELECT a.sicob_id as id_a, 
        (
        	(ST_Dump( 
    			COALESCE(
        			ST_Difference(
            			the_geom, 
                		(SELECT ST_Union( b.the_geom ) 
                 			FROM borde b
						)
					), 
            		a.the_geom
				) 
			)).geom
        ) as the_geom 
		FROM diff a
	)    
	SELECT * FROM cleandiff
    WHERE trunc(st_area(the_geom)*10000000000) > 0
    ';

    END IF;

    RAISE DEBUG 'Running %', sql;

    --EXECUTE 'CREATE TABLE ' || tbl_subfixDiff || ' AS ' || sql;
    EXECUTE 'CREATE TEMPORARY TABLE ' || tbl_subfixDiff || ' ON COMMIT DROP AS ' || sql;
    
    GET DIAGNOSTICS row_cnt = ROW_COUNT; -->obteniendo la cantidad de poligonos que NO se intersectan.
    _out := _out::jsonb || jsonb_build_object('features_diff_cnt', row_cnt); 
    
    IF row_cnt > 0 THEN 
    	EXECUTE 'INSERT INTO ' || tbl_subfixResult || '(id_a, source_a, source_b, the_geom)
    	SELECT id_a, ''' || a::text || ''' as source_a, ''' || b::text || ''' as source_b, the_geom FROM ' || tbl_subfixDiff;
	END IF;	



END IF; -->AGREGANDO POLIGONOS QUE NO SE INTERSECTAN

--AGREGANDO GEOINFORMACION
IF COALESCE((_opt->>'add_geoinfo')::boolean,FALSE) = TRUE
	OR COALESCE((_opt->>'add_sup_total')::boolean,FALSE) = TRUE THEN
	PERFORM sicob_add_geoinfo_column(tbl_subfixResult);
	PERFORM sicob_update_geoinfo_column(tbl_subfixResult);
    IF _min_sup > 0 THEN
    	--FILTRANDO LOS POLIGONOS QUE NO CUMPLEN LA SUPERFICIE MINIMA
        sql := 'DELETE FROM ' || tbl_subfixResult || ' WHERE sicob_sup < ' || _min_sup::text;
        RAISE DEBUG 'Running %', sql;
    	EXECUTE sql;
    END IF;
    IF COALESCE((_opt->>'add_sup_total')::boolean,FALSE) = TRUE
    AND (_out->>'features_inters_cnt')::integer > 0  THEN
    	EXECUTE '
        SELECT sum(sicob_sup) as sicob_sup_total 
        FROM(
        	SELECT (dp).path[1] as id, (dp).geom as the_geom, round((ST_Area(ST_Transform((dp).geom,
            	SICOB_utmzone_wgs84((dp).geom)))/10000)::numeric,4)  as sicob_sup
            FROM (
            	SELECT st_dump(t.the_geom) as dp
                	FROM(
                    	SELECT source_b, ST_Multi(ST_Union(f.the_geom)) as the_geom
                        FROM ' || tbl_subfixResult || ' As f
                        WHERE id_b IS NOT NULL
                        GROUP BY source_b
					) t
			)u
		)v;' INTO _sup;
    	--EXECUTE 'SELECT sum(sicob_sup) as sup_total FROM ' || tbl_subfixResult || ' WHERE id_b IS NOT NULL' INTO _sup;
        _out := _out::jsonb || jsonb_build_object('sicob_sup_total', _sup); 
    END IF;
END IF;

_out := _out::jsonb || jsonb_build_object('lyr_over', tbl_subfixResult); 

RETURN _out;

EXCEPTION
WHEN others THEN
            RAISE EXCEPTION 'geoSICOB (sicob_overlap):a:% |condition_a: % | b:%>> % (%) | sql: %', a|| '(schema:' || sch_nameA || ' table:'|| tbl_nameA || ')', COALESCE((_opt->>'condition_a')::text , '') , b,  SQLERRM, SQLSTATE, sql;	
END;
$function$
 