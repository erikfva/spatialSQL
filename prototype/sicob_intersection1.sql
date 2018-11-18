CREATE OR REPLACE FUNCTION public.sicob_intersection1(_opt json)
 RETURNS json
 LANGUAGE plpgsql
AS $function$
DECLARE 
  a TEXT;
  b TEXT;
  _condition_a text; _condition_b text;
  _subfixresult text;
  _schema text;
  _paralell_workers int;
---------------------------------
  _paralell_opt json := '{}';
  _partial_result json := '{}';
 
 sql text;

 tbl_nameA text; sch_nameA text;
 tbl_nameB text; sch_nameB text;
 
 row_cnt integer;
 __a text; __b text;
 _out json := '{}';
 -------------------------------

BEGIN
---------------------------
--PRE - CONDICIONES
---------------------------
--> Los elementos de las capas de entradas deben ser poligonos simples (POLYGON). No deben ser Multipolygon o colecciones.
--> El campo identificador unico de cada capa debe ser "sicob_id".
---------------------------
--PARAMETROS DE ENTRADA
---------------------------
--> a : capa de poligonos que se intersectara con la capa "b".
--> condition_a (opcional): Filtro para los datos de "a". Si no se especifica, se toman todos los registros.
--> b : capa de poligonos que se intersectara con la capa "a".
--> condition_b (opcional): Filtro para los datos de "b". Si no se especifica, se toman todos los registros.
--> subfix (opcional): texto adicional que se agregará al nombre de la capa resultante (a_inter_b).
--> schema (opcional): esquema del la BD donde se creará la capa resultante. Si no se especifica se creará en "temp".
--> temp (opcional true/false): Indica si la capa resultante será temporal mientras dura la transacción. Esto se requiere cuando el resultado es utilizado como capa intermedia en otros procesos dentro de la misma transacción. Por defecto es FALSE.
-->filter_overlap (opcional): Indica si se filtraran poligonos solapados en la capa resultado. Por defecto es TRUE.
--> paralell_workers (opcional): Si se especifica y es mayor que 1, el sistema realizara paralelamente el trabajo particonando la capa "a" en el numero de bloques indicado en esta variable.

---------------------------
--VALORES DEVUELTOS
---------------------------
--> lyr_intersected : capa resultante del ajuste de bordes. Solo se incluyen los campos: sicob_id, id_a, source_a, id_b, source_b, the_geom.
--> features_inters_cnt: Cantidad de poligonos resultantes.

--_opt := '{"a":"processed.f20171005fgcbdae84fb9ea1_nsi","b":"coberturas.parcelas_tituladas","subfix":"", "schema":"temp", "paralell_workers":"3"}';
RAISE NOTICE '_opt: %', _opt::text;

a := (_opt->>'a')::TEXT;
_condition_a := COALESCE((_opt->>'condition_a')::text, 'TRUE');

b := (_opt->>'b')::TEXT;
_condition_b := COALESCE((_opt->>'condition_b')::text, 'TRUE');

_subfixresult := COALESCE(_opt->>'subfix',''); 
_schema := COALESCE(_opt->>'schema','temp');

_paralell_workers := COALESCE( (_opt->>'paralell_workers')::int, 1);

SELECT * FROM sicob_split_table_name(a::text) INTO sch_nameA, tbl_nameA;
SELECT * FROM sicob_split_table_name(b::text) INTO sch_nameB, tbl_nameB;

IF _paralell_workers > 1 THEN
	_paralell_opt := _opt::jsonb || 
    	jsonb_build_object(
        	'condition_a', _condition_a || ' AND <chunk_condition>',
            'subfix',_subfixresult || '<chunk_id>',
            'schema','temp',
            'temp',false,
            'paralell_workers',0
        );
    RAISE NOTICE 'paralell_opt: %', _paralell_opt::text;
    select sicob_execute(
    		'SELECT * FROM sicob_intersection('|| QUOTE_LITERAL(_paralell_opt) ||'::json)',
        	('{"table_to_chunk":"' || a || '", "workers":"' || _paralell_workers || '","partitions":"' || _paralell_workers || '"}')::json
        ) INTO _out;
    sql := '';
    FOR _partial_result IN SELECT * FROM json_array_elements(_out) LOOP
    	IF sql <> '' THEN
        	sql := sql || ' UNION ALL ';
        END IF;
        sql := sql || 'SELECT id_a,source_a,id_b,source_b FROM ' ||  ((_partial_result->'result')->>'lyr_intersected')::text;  	
    END LOOP;
    
    sql := 'SELECT row_number() over() AS sicob_id, id_a, source_a, id_b,source_b 
    		FROM (' || sql || ') t
            ORDER BY t.id_a';
   
    --RAISE NOTICE '%', sql;
ELSE
    --> CREANDO EL SUBCONJUNTO DE "b" QUE SE INTERSECTA CON "a"
    __b := '__b';

    sql := '
    SELECT DISTINCT b.sicob_id,b.the_geom
    FROM 
    ' || b::text || ' b INNER JOIN ' || a::text || ' a
    ON (
        ' || _condition_a || ' AND ' || _condition_b || ' AND st_intersects(a.the_geom, b.the_geom) AND NOT ST_Touches(a.the_geom, b.the_geom)
    )';

    RAISE DEBUG 'Running %', sql;
    EXECUTE 'DROP TABLE IF EXISTS ' || __b;
    EXECUTE 'CREATE TEMPORARY TABLE ' || __b || ' ON COMMIT DROP AS ' || sql;

    GET DIAGNOSTICS row_cnt = ROW_COUNT; -->obteniendo la cantidad de poligonos que se intersectan	

    sql := '';
    IF row_cnt > 0 THEN
        
        sql := 'DROP INDEX IF EXISTS ' || __b || '_geomidx';
        RAISE DEBUG 'Running %', sql;
        EXECUTE sql;
                    
        sql := 'CREATE INDEX ' || __b || '_geomidx
        ON ' || __b || ' 
        USING GIST (the_geom) ';
        RAISE DEBUG 'Running %', sql;
        EXECUTE sql;
        
        -----------------------------------------
        --GENERANDO LA INTERSECCION
        -----------------------------------------
           
        sql := '
        WITH 
        a_intersect_b AS (
          SELECT 
              row_number() over() AS ___o, 
              a.sicob_id as id_a, 
              b.sicob_id as id_b,
              CASE 
                  WHEN ST_CoveredBy(a.the_geom, b.the_geom) 
                      THEN a.the_geom 
                  WHEN ST_CoveredBy(b.the_geom, a.the_geom) 
                      THEN b.the_geom 
                  ELSE 
                      sicob_intersection(
                        a.the_geom, 
                        b.the_geom,
                        ''POLYGON''
                     )
              END AS the_geom
          FROM  ' || a::text || ' a,' || __b || ' b
          WHERE 
         ' || _condition_a || ' AND 
          st_intersects(a.the_geom, b.the_geom)
          AND NOT ST_Touches(a.the_geom, b.the_geom)
        ),
        overlayed AS (
          --filtrando los polignos que se sobreponenen en la misma capa
          SELECT 
          CASE WHEN st_area(b1.the_geom) < st_area(b2.the_geom) THEN
              b1.___o
          ELSE
              b2.___o
          END
          FROM
              a_intersect_b b1,
              a_intersect_b b2
          WHERE 
              ' || COALESCE((_opt->>'filter_overlap')::text, 'TRUE') || ' AND
              b1.___o <> b2.___o and
              b1.___o < b2.___o and
              ST_Intersects(b1.the_geom,b2.the_geom) 
              AND NOT ST_Touches(b1.the_geom, b2.the_geom)
        )
        SELECT 
          row_number() over() AS sicob_id, 
          id_a, ''' || a::text || '''::text as source_a,
          id_b, ''' || b::text || '''::text as source_b,  
          the_geom 
        FROM 
          a_intersect_b r
        WHERE
          NOT EXISTS (SELECT ___o FROM overlayed t WHERE t.___o = r.___o)
          AND trunc(st_area(the_geom)*10000000000) > 0
        ';   
        RAISE DEBUG 'Running %', sql;  
    END IF; 
END IF;

    
    IF sql = '' THEN
    	--> Si no se genero una nueva cobertura devuelve 0.
    	RETURN ('{"lyr_intersected":"","features_inters_cnt":"0"}')::json;
    END IF;
    
    __a := tbl_nameA || '_inter_' ||  tbl_nameB || _subfixresult;
	IF COALESCE((_opt->>'temp')::boolean, FALSE) THEN
    	EXECUTE 'DROP TABLE IF EXISTS ' || __a;
    	EXECUTE 'CREATE TEMPORARY TABLE ' || __a || ' ON COMMIT DROP AS ' || sql;
    ELSE
    	__a := _schema || '.' || __a;
        EXECUTE 'DROP TABLE IF EXISTS ' || __a;
    	EXECUTE 'CREATE UNLOGGED TABLE ' || __a || ' AS ' || sql;
    END IF;
    GET DIAGNOSTICS row_cnt = ROW_COUNT;

    EXECUTE 'ALTER TABLE ' || __a || ' ADD PRIMARY KEY (sicob_id);'; 
    EXECUTE 'CREATE INDEX ' || tbl_nameA || '_inter_' ||  tbl_nameB || _subfixresult || '_id_a ON ' || __a || ' USING btree (id_a);';
    
    /*
    sql := 'DROP INDEX IF EXISTS ' || tbl_nameA || _subfixresult || '_geomidx';
    RAISE DEBUG 'Running %', sql;
    EXECUTE sql;
            
    sql := 'CREATE INDEX ' || tbl_nameA || _subfixresult || '_geomidx
    ON ' || __a || ' 
    USING GIST (the_geom) ';
    RAISE DEBUG 'Running %', sql;
    EXECUTE sql;    
    */
    
    RETURN ('{"lyr_intersected":"' || __a || '","features_inters_cnt":"' || row_cnt || '"}')::json;
    

EXCEPTION
WHEN others THEN
            RAISE EXCEPTION 'geoSICOB (sicob_intersection) % , % , _opt: % | sql: %', SQLERRM, SQLSTATE, _opt, sql;	
END;
$function$
 