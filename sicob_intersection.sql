SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_intersection(_opt json)
 RETURNS json
 LANGUAGE plpgsql
AS $function$
DECLARE
/* 
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

 sicob_sup_total real := 0;
 -------------------------------
 */
  _out json := '{}';

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
--> add_fields (opcional true/false): Agrega los campos de "b" en el resultado.
--> add_sup (opcional true/false): Calcular la superficie en ha. Por defecto es "true".
---------------------------
--VALORES DEVUELTOS
---------------------------
--> lyr_intersected : capa resultante del ajuste de bordes. Solo se incluyen los campos: sicob_id, id_a, source_a, id_b, source_b, the_geom.
--> features_inters_cnt: Cantidad de poligonos resultantes.

--_opt := '{"a":"processed.f20171005fgcbdae84fb9ea1_nsi","b":"coberturas.parcelas_tituladas","subfix":"", "schema":"temp"}';

--_opt := '{"a" : "temp.f20170718fagebdcf580ac83_nsi_tit_tioc_adjust", "b" : "coberturas.predios_proceso_geosicob_geo_201607", "condition_b" : "TRUE", "schema" : "temp", "filter_overlap" : true}';
--_opt := '{"a" : "processed.f20181128dfgbceacd93dce9_fixt", "b" : "coberturas.pop_uso_vigente", "condition_b" : "TRUE", "schema" : "temp", "temp" : false, "filter_overlap" : false, "add_sup_total" : true, "min_sup" : 0, "add_geoinfo" : false}';

--RAISE NOTICE '_opt: %', _opt::text;

WITH 
_params AS (
SELECT 
    COALESCE((opt->>'a')::text,'') as a,
    (SELECT table_name FROM sicob_split_table_name((opt->>'a')::text)) as tbl_a,
    COALESCE((opt->>'condition_a')::text,'TRUE') as condition_a,
    COALESCE((opt->>'b')::text,'') as b,
    COALESCE((opt->>'condition_b')::text,'TRUE') as condition_b,
    COALESCE((opt->>'add_fields')::boolean,FALSE) as add_fields,
    COALESCE((opt->>'temp')::boolean,FALSE) as _temp,
    COALESCE((opt->>'subfix')::text,'') || '_a_inter_b' as subfix,
    COALESCE((opt->>'schema')::text,'temp') as _schema,
    COALESCE((opt->>'min_sup')::real,0::real) as min_sup,
    COALESCE((opt->>'add_sup')::boolean,TRUE) as add_sup,
    COALESCE((opt->>'add_sup_total')::boolean,FALSE) as add_sup_total
 FROM (
 	SELECT 
    /* USAR PARA PRUEBAS */
/*
    json_build_object(
    	'a', 'uploads.f20190515fgdcbead21d39cd',
        'b', 'coberturas.predios_titulados',
        'add_fields', TRUE,
        'add_sup_total', TRUE,
        'add_diff', TRUE,
        'filter_overlap', FALSE,
        'min_sup', 1
    ) 
*/
    _opt 
    as opt
 ) params
),
a_inter_b AS (
	SELECT
	sicob_executesql('
      WITH
      a_inter_b AS (    
        select 
          row_number() over() as sicob_id,
          a.sicob_id as id_a,
          b.sicob_id as id_b,
          ''' || a || ''' AS source_a,
          ''' || b || ''' AS source_b,' ||
          CASE WHEN add_fields THEN
              sicob_no_geo_column(b,'{sicob_id,id_a,source_a,id_b,source_b, sicob_sup, sicob_utm}','b.') || ','
          ELSE
              ''
          END ||
          CASE WHEN add_sup THEN
          '(st_area(ST_Transform( st_intersection(a.the_geom,b.the_geom), SICOB_utmzone_wgs84(st_intersection(a.the_geom,b.the_geom)) ))/10000) as sicob_sup,'
          ELSE
          ''
          END ||
          'st_intersection(a.the_geom,b.the_geom) as the_geom
        from 
        ' || a::text || ' a 
        INNER JOIN ' || b::text || ' b
        ON ( ((' || condition_a || ' ) AND (' || condition_b || ')) AND st_intersects(a.the_geom, b.the_geom) AND NOT ST_Touches(a.the_geom, b.the_geom))
      ),
      a_inter_b_fixsliver AS (
      	SELECT 
        * 
        FROM 
        a_inter_b
        WHERE 
          trunc(
			st_area(
				st_buffer( 
                  st_buffer( the_geom ,-0.000001,''endcap=flat join=round quad_segs=1''), --> umbral de 0.5cm
                  0.000001,''endcap=flat join=round quad_segs=1''
                )
			)*10000000000
          ) > 0
      )
      SELECT * FROM a_inter_b_fixsliver',
      json_build_object(
            'table_out', CASE WHEN _temp THEN 't' || MD5(random()::text) ELSE _schema || '.' || tbl_a || subfix END ,
            'temp', _temp,
            'create_index', true
      )
	)->>'table_out' as lyr_inter
    FROM
    _params
),
a_inter_b_diss AS (
    SELECT  
      sicob_dissolve(
      	json_build_object(
        	'lyr_in', lyr_inter,
            'add_sup', TRUE
        )
      ) as res_diss
    FROM
      a_inter_b
),
result AS (
  SELECT 
      lyr_inter as lyr_intersected,
      sicob_executesql('SELECT count(*) as total FROM ' || lyr_inter ,json_build_object('return_scalar',TRUE))->>'total' as features_inters_cnt,
      CASE WHEN (SELECT add_sup_total FROM _params) THEN
          (SELECT res_diss->>'lyr_diss' FROM a_inter_b_diss)
      ELSE
          null
      END as lyr_diss,
      CASE WHEN (SELECT add_sup_total FROM _params) THEN
          (SELECT res_diss->>'sicob_sup_total' FROM a_inter_b_diss)::real
      ELSE
          null
      END as inters_sup
  FROM a_inter_b
)
SELECT json_strip_nulls(row_to_json(t)) FROM result t INTO _out;
  
    RETURN _out;
    

EXCEPTION
WHEN others THEN
            RAISE EXCEPTION 'geoSICOB (sicob_intersection) % , % , _opt: % ', SQLERRM, SQLSTATE, _opt;	
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