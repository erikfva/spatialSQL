SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_intersection(_opt json)
 RETURNS json
 LANGUAGE plpgsql
AS $function$
DECLARE 
  a TEXT;
  b TEXT;
  _condition_a text; _condition_b text;
  _subfixresult text;
  _schema text;

---------------------------------

  sql text;

 tbl_nameA text; oidA text; oidB text; sch_nameA text;
 tbl_nameB text; sch_nameB text;
 
 row_cnt integer;
 __a text; __b text; a__b TEXT;
 _out json := '{}';
 sicob_sup_total real := 0;
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
--> subfix (opcional): texto adicional que se agregarï¿½ al nombre de la capa resultante (a_inter_b).
--> schema (opcional): esquema del la BD donde se crearï¿½ la capa resultante. Si no se especifica se crearï¿½ en "temp".
--> temp (opcional true/false): Indica si la capa resultante serï¿½ temporal mientras dura la transacciï¿½n. Esto se requiere cuando el resultado es utilizado como capa intermedia en otros procesos dentro de la misma transacciï¿½n. Por defecto es FALSE.
-->filter_overlap (opcional): Indica si se filtraran poligonos solapados en la capa resultado. Por defecto es TRUE.
--> add_sup_total (opcional true/false): Calcula y devuelve la superficie total sobrepuesta en hectareas.
--> min_sup (opcional): Superficie minima (en hectareas) permitida para los poligonos de la capa resultado.
--> add_geoinfo (opcional true/false): Agrega o no la informaciï¿½n del cï¿½digo de proyecciï¿½n UTM 'sicob_utm', superficie (ha) y 'sicob_sup' para cada polï¿½gono.
---------------------------
--VALORES DEVUELTOS
---------------------------
--> lyr_intersected : capa resultante del ajuste de bordes. Solo se incluyen los campos: sicob_id, id_a, source_a, id_b, source_b, the_geom.
--> features_inters_cnt: Cantidad de poligonos resultantes.

--_opt := '{"a":"processed.f20171005fgcbdae84fb9ea1_nsi","b":"coberturas.parcelas_tituladas","subfix":"", "schema":"temp"}';

--_opt := '{"a" : "temp.f20170718fagebdcf580ac83_nsi_tit_tioc_adjust", "b" : "coberturas.predios_proceso_geosicob_geo_201607", "condition_b" : "TRUE", "schema" : "temp", "filter_overlap" : true}';
--_opt := '{"a" : "processed.f20181128dfgbceacd93dce9_fixt", "b" : "coberturas.pop_uso_vigente", "condition_b" : "TRUE", "schema" : "temp", "temp" : false, "filter_overlap" : false, "add_sup_total" : true, "min_sup" : 0, "add_geoinfo" : false}';

--RAISE NOTICE '_opt: %', _opt::text;

a := (_opt->>'a')::TEXT;
_condition_a := COALESCE((_opt->>'condition_a')::text, 'TRUE');

b := (_opt->>'b')::TEXT;
_condition_b := COALESCE( (_opt->>'condition_b')::text , 'TRUE');

_subfixresult := COALESCE(_opt->>'subfix',''); 
_schema := COALESCE(_opt->>'schema','temp');

IF a = '' OR b = '' THEN
   --> No existe interseccion. Retornar vacio.
	RETURN ('{"lyr_intersected":"","features_inters_cnt":"0"}')::json;
END IF;

SELECT *,a::regclass::oid FROM sicob_split_table_name(a::text) INTO sch_nameA, tbl_nameA, oidA ;
SELECT *,b::regclass::oid FROM sicob_split_table_name(b::text)  INTO sch_nameB, tbl_nameB, oidB;

sql := '
  WITH 
  b_inter_a AS (
      select  
          distinct b.sicob_id, b.the_geom
      from 
      ' || a || ' a 
      INNER JOIN ' || b || ' b
      ON (st_intersects(a.the_geom, b.the_geom) AND NOT ST_Touches(a.the_geom, b.the_geom))
  ),
  grouppolygons AS (
      SELECT
          ''1'' as idgroup,
          st_multi(
              st_union(
                  the_geom                
              )
          )
          AS the_geom
      FROM
         b_inter_a
      GROUP BY idgroup
  ),
  buff AS (
      SELECT 
          idgroup,
          st_buffer(
              the_geom,
              5::float/111000, ''join=mitre mitre_limit=5.0''
          ) as the_geom
      FROM 
          grouppolygons
  ),
  b_inter_a_diss AS (
      SELECT 
          idgroup, st_union(the_geom) AS the_geom
      FROM (
          SELECT
            idgroup,
            st_buffer(
                st_makepolygon(
                    st_exteriorring(
                        (st_dump(the_geom)).geom
                    )
                ),
                -5::float/111000, ''join=mitre mitre_limit=5.0''
            ) as the_geom
          FROM  buff
      ) t
      GROUP BY idgroup
  ),
  a_inter_b AS (
      SELECT 
          a.sicob_id, st_intersection(a.the_geom, ( SELECT the_geom FROM b_inter_a_diss limit 1) ) as the_geom
      FROM 
      ' || a || ' a
  ),
  a_inter_b_fixsliver AS (
      SELECT 
      sicob_id,
      the_geom
      FROM a_inter_b
      WHERE 
      trunc(st_area(the_geom)*10000000000) > 0
  )
  SELECT 
      sicob_id, the_geom
  FROM 
      a_inter_b_fixsliver
';


--> CREANDO LOS PARES de indices (ai,bi) DE LOS POLIGONOS QUE SE INTERSECTAN
    a__b := 'a__b';

    sql := '
    SELECT DISTINCT a.sicob_id as id_a, b.sicob_id as id_b
    FROM 
    ' || b::text || ' b INNER JOIN ' || a::text || ' a
    ON (
        ' || _condition_a || ' AND ' || _condition_b || ' AND st_intersects(a.the_geom, b.the_geom) AND NOT ST_Touches(a.the_geom, b.the_geom)
    )';

    RAISE DEBUG 'Running %', sql;
    EXECUTE 'DROP TABLE IF EXISTS ' || a__b;
    EXECUTE 'CREATE TEMPORARY TABLE ' || a__b || ' ON COMMIT DROP AS ' || sql;

    GET DIAGNOSTICS row_cnt = ROW_COUNT; -->obteniendo la cantidad de intersecciones.	

IF row_cnt = 0 THEN
    --> No existe interseccion. Retornar vacio.
    	sql := 'SELECT null::integer as sicob_id, a.id_a, a.id_b, ' || 
            CASE WHEN COALESCE((_opt->>'add_sup_total')::boolean, FALSE)
            	OR COALESCE((_opt->>'add_geoinfo')::boolean, FALSE)  
                OR COALESCE((_opt->>'min_sup')::real, 0) > 0 THEN
                'NULL::integer as sicob_utm,
                NULL::real as sicob_sup,' 
            ELSE 
                '' 
            END || ' 
        ''POLYGON EMPTY''::geometry as the_geom 
        FROM ' || a__b || ' a';
ELSE

--> Generando los poligonos de interseccion.

	--Producto1.- Obteniendo los poligonos de "a" que son completamente cubiertos por "b".
    sql := '
        SELECT DISTINCT inters.id_a, inters.id_b, a.the_geom
        FROM ' || a__b || ' inters 
        INNER JOIN ' || a::text || ' a ON (a.sicob_id = inters.id_a)
        INNER JOIN ' || b::text || ' b ON (b.sicob_id = inters.id_b)
        WHERE (ST_CoveredBy(a.the_geom,b.the_geom) )';
    /*        
    EXECUTE 'DROP TABLE IF EXISTS ' || tbl_nameA || '_fullcovered';
    EXECUTE 'CREATE TEMPORARY TABLE ' || tbl_nameA || '_fullcovered ON COMMIT DROP AS ' || sql;
      */  
	--Producto2.- Obteniendo los poligonos de "b" que son completamente cubiertos por "a".
    sql := '
        SELECT DISTINCT inters.id_a, inters.id_b, a.the_geom
        FROM ' || a__b || ' inters 
        INNER JOIN ' || a::text || ' a ON (a.sicob_id = inters.id_a)
        INNER JOIN ' || b::text || ' b ON (b.sicob_id = inters.id_b)
        WHERE NOT EXISTS (SELECT id_a FROM ' || tbl_nameA || '_fullcovered z WHERE z.id_a = inters.id_a) AND (ST_CoveredBy(b.the_geom,a.the_geom) = TRUE )';
    /*        
    EXECUTE 'DROP TABLE IF EXISTS ' || tbl_nameB || '_fullcovered';
    EXECUTE 'CREATE TEMPORARY TABLE ' || tbl_nameB || '_fullcovered ON COMMIT DROP AS ' || sql;     
    */
    --Producto3.- Obteniendo el corte de los poligonos que son parcialmente cubiertos.
    sql := '
    WITH
    a_intersect_b AS (
        SELECT
        	row_number() over() AS ___o, 
        	inters.id_a, 
            inters.id_b, 
            sicob_intersection(
                        a.the_geom, 
                        b.the_geom,
                        ''POLYGON''
            ) as the_geom
        FROM ' || a__b || ' inters 
        INNER JOIN ' || a::text || ' a ON (a.sicob_id = inters.id_a)
        INNER JOIN ' || b::text || ' b ON (b.sicob_id = inters.id_b)
        /*
        WHERE 
        	NOT EXISTS (SELECT id_a FROM ' || tbl_nameA || '_fullcovered y WHERE y.id_a = inters.id_a) AND
            NOT EXISTS (SELECT id_b FROM ' || tbl_nameB || '_fullcovered z WHERE z.id_b = inters.id_b) */ 
    ),
	overlayed AS (
        --filtrando los polignos que se sobreponenen en la misma capa
            SELECT 
            CASE WHEN st_area(b1.the_geom) < st_area(b2.the_geom) THEN
                b1.___o
            ELSE
                b2.___o
            END as ___o
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
    SELECT id_a, id_b, the_geom
    FROM a_intersect_b inters
    WHERE 
    NOT EXISTS (SELECT ___o FROM overlayed ov WHERE ov.___o = inters.___o)
    ';
	    
    EXECUTE 'DROP TABLE IF EXISTS ' || tbl_nameA || '_inters';
    EXECUTE 'CREATE TEMPORARY TABLE ' || tbl_nameA || '_inters ON COMMIT DROP AS ' || sql; 
    
    --Consolidando los 3 productos.
    sql := ' 
   	SELECT 
			id_a, id_b, (dp).geom as the_geom
        FROM (
        	SELECT id_a, id_b, st_dump(the_geom) as dp FROM ' || tbl_nameA || '_inters' || ' x
        ) w      
    ';
    sql := '
    	WITH
        _results AS (
            SELECT 
                id_a,id_b,the_geom' || 
                CASE WHEN COALESCE((_opt->>'add_sup_total')::boolean, FALSE)
                	OR COALESCE((_opt->>'add_geoinfo')::boolean, FALSE) 
                    OR COALESCE((_opt->>'min_sup')::real, 0) > 0 THEN 
                    ', ST_Transform(the_geom, SICOB_utmzone_wgs84(the_geom) ) as the_geomutm,
                    SICOB_utmzone_wgs84(the_geom) as sicob_utm ' 
                ELSE 
                    '' 
                END || ' 
            FROM (
                ' || sql || '
            ) t
            WHERE 
            	trunc(st_area(t.the_geom)*10000000000) > 0
        )
        SELECT 
        	row_number() over() AS sicob_id,
            id_a,id_b,the_geom' || 
            CASE WHEN COALESCE((_opt->>'add_sup_total')::boolean, FALSE) 
            	OR COALESCE((_opt->>'add_geoinfo')::boolean, FALSE) 
                OR COALESCE((_opt->>'min_sup')::real, 0) > 0 THEN 
                ', the_geomutm, sicob_utm, st_area(the_geomutm)/10000 as sicob_sup' 
            ELSE 
                '' 
            END || '
        FROM
        	_results'
        || 
        CASE WHEN COALESCE((_opt->>'min_sup')::real, 0) > 0 THEN 
        	' WHERE st_area(the_geomutm)/1000 >= ' || (_opt->>'min_sup')::text
        ELSE 
        	'' 
        END ||
        ' ORDER BY id_a, id_b';
END IF;
	
	RAISE DEBUG 'Running %', sql;    

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
    EXECUTE 'DROP INDEX IF EXISTS id_a_' || oidA || '_inter_' ||  oidB || _subfixresult;
    RAISE DEBUG 'CREANDO INDICE... : %', 'id_a_' || oidA || '_inter_' ||  oidB || _subfixresult;
    EXECUTE 'CREATE INDEX id_a_' || oidA || '_inter_' ||  oidB || _subfixresult || ' ON ' || __a || ' USING btree (id_a);';

    EXECUTE 'DROP INDEX IF EXISTS the_geom_' || oidA || '_inter_' ||  oidB || _subfixresult;
    RAISE DEBUG 'CREANDO INDICE... : %', 'the_geom_' || oidA || '_inter_' ||  oidB || _subfixresult;
    EXECUTE 'CREATE INDEX the_geom_' || oidA || '_inter_' ||  oidB || _subfixresult || ' ON ' || __a || ' USING GIST (the_geom);';
    EXECUTE 'CLUSTER ' || __a || ' USING the_geom_' || oidA || '_inter_' ||  oidB || _subfixresult;
    
    --Corriguiendo geometrias con errores 
    /*
    IF row_cnt > 0 THEN
    	--PERFORM sicob_makevalid(__a);
        EXECUTE 'UPDATE ' || __a || ' SET the_geomutm = st_makevalid(the_geomutm) 
    	WHERE st_isvalid(the_geomutm) = FALSE';
    END IF;
    */
    
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
    
 	_out := ('{"lyr_intersected":"' || __a || '","features_inters_cnt":"' || row_cnt || '"}')::json;

	   
    --CALCULANDO LA SUPERFICIE TOTAL SOBREPUESTA de "a" en HA.
    IF COALESCE((_opt->>'add_sup_total')::boolean, FALSE) THEN
    	IF row_cnt > 0 THEN
        	/*
        	sql := '
            	SELECT sum(sicob_sup) as sicob_sup FROM ' || 
                sicob_fix_si(__a)
                || '
            ';
            */
            sql := '
              WITH
              fix_sliver AS (
                SELECT
                sicob_id,
                st_buffer(
                  st_buffer(
                      the_geom, 0.00000000001
                  ), -0.00000000001
                ) 
                as the_geom
                FROM
                ' || __a || '
              ),
              diss AS (
                SELECT
                st_union(the_geom) as the_geom
                FROM
                fix_sliver
              )
              select 
              round((ST_Area(ST_Transform(the_geom, SICOB_utmzone_wgs84(the_geom)))/10000)::numeric,5)
              from diss
            ';
            EXECUTE sql INTO sicob_sup_total;
        ELSE
        	sicob_sup_total := 0;
        END IF;
        _out := _out::jsonb || jsonb_build_object('inters_sup',sicob_sup_total);
    END IF;
  
    RETURN _out;
    

EXCEPTION
WHEN others THEN
            RAISE EXCEPTION 'geoSICOB (sicob_intersection) % , % , _opt: % | sql: % | condition_b: %', SQLERRM, SQLSTATE, _opt, sql, _condition_b;	
END;
$function$
;CREATE OR REPLACE FUNCTION public.sicob_intersection(param_geoms geometry[])
 RETURNS geometry
 LANGUAGE plpgsql
AS $function$
DECLARE result geometry := param_geoms[1];
BEGIN
-- an intersection with an empty geometry is an empty geometry
  IF array_upper(param_geoms,1) > 1 AND NOT ST_IsEmpty(param_geoms[1]) THEN
    FOR i IN 2 .. array_upper(param_geoms, 1) LOOP
      result := ST_Intersection(result,st_buffer(param_geoms[i],0.0000000000001 ));
      IF ST_IsEmpty(result) THEN
        EXIT;
      END IF;
    END LOOP;
  END IF;
  RETURN result;
END;
$function$
;CREATE OR REPLACE FUNCTION public.sicob_intersection(geom1 geometry, geom2 geometry, geom_type text)
 RETURNS geometry
 LANGUAGE plpgsql
 STABLE
AS $function$
DECLARE 
  intersected geometry;
  sql text;
  intersected_type text;
  geom_code integer;
  alternate_type text;
	exterior_ring geometry;
BEGIN

IF st_isvalid(geom1) = FALSE THEN
	SELECT st_makevalid(geom1) INTO geom1;
END IF;

 if geom_type = 'POINT' THEN
  geom_code := 1;
  alternate_type := 'MULTIPOINT';
 elsif geom_type = 'LINESTRING' THEN
  geom_code := 2;
  alternate_type := 'MULTILINESTRING';
 elsif geom_type = 'POLYGON' THEN
  geom_code := 3;
  alternate_type := 'MULTIPOLYGON';
 else
  raise 'geom_type must be one of POLYGON, LINESTRING, or POINT';
 end if;


SELECT ST_Intersection(geom1,geom2) INTO intersected;


SELECT  ST_Collect(ST_ExteriorRing(the_geom))  INTO  exterior_ring
	FROM (
        SELECT (dp).path[1] as gid, (dp).geom as the_geom 
        FROM(
            SELECT st_dump(geom1) as dp
            ) t        
    ) As u
GROUP BY gid;

SELECT   ST_Difference( intersected, exterior_ring ) INTO intersected;

/*
SELECT   ST_Difference( intersected,  st_buffer( exterior_ring,0.000000001) ) INTO intersected;
 
SELECT
st_snap(
	intersected,
	exterior_ring,
    0.000001
) INTO intersected;
*/

SELECT geometrytype(intersected) INTO intersected_type;

 if intersected_type = 'GEOMETRYCOLLECTION' then
  select ST_CollectionExtract(intersected,geom_code) INTO intersected;
  SELECT geometrytype(intersected) INTO intersected_type;
 end if;

 IF intersected_type in (geom_type,alternate_type) then
  return intersected;
 else
  return null;
 end if;
 

EXCEPTION
	WHEN others THEN
		RAISE EXCEPTION 'geoSICOB (sicob_intersection) geom1: % , geom2: % , %, %, sql: %', st_astext(geom1), st_astext(geom2), SQLERRM, SQLSTATE, sql;	 
END;
$function$
 