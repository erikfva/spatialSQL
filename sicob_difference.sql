SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_difference(_opt json)
 RETURNS json
 LANGUAGE plpgsql
AS $function$
DECLARE 
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
--> a : capa de poligonos de los cuales se quita la parte que se intersecta con "b".
--> condition_a (opcional): Filtro para los datos de "a". Si no se especifica, se toman todos los registros.
--> b : capa de poligonos que se quitaran cuando se intersecte con "a".
--> condition_b (opcional): Filtro para los datos de "b". Si no se especifica, se toman todos los registros.
--> subfix (opcional): texto adicional que se agregarï¿½ al nombre de "a" para formar el nombre de la capa resultante. Si no se especifica por defecto es "_diff".
--> schema (opcional): esquema del la BD donde se crearï¿½ la capa resultante. Si no se especifica se crearï¿½ en "temp".
--> temp (opcional true/false) : Indica si la capa resultante serï¿½ temporal mientras dura la transacciï¿½n. Esto se requiere cuando el resultado es utilizado como capa intermedia en otros procesos dentro de la misma transacciï¿½n. Por defecto es FALSE.
-->filter_overlap (opcional): Indica si se filtraran poligonos solapados en la capa resultado. Por defecto es TRUE.
--> add_sup_total (opcional true/false): Calcula y devuelve la superficie total sobrepuesta en hectareas.
--> min_sup (opcional): Superficie minima (en hectareas) permitida para los poligonos de la capa resultado.
--> add_geoinfo (opcional true/false): Agrega o no la informaciï¿½n del cï¿½digo de proyecciï¿½n UTM 'sicob_utm', superficie (ha) y 'sicob_sup' para cada polï¿½gono.
--> add_fields (opcional true/false): Agrega los campos de "a" en el resultado.
---------------------------
--VALORES DEVUELTOS
---------------------------
--> lyr_difference : capa resultante con elementos de "a" que no se intersectan con "b". Solo se incluyen dos campos: sicob_id,the_geom.

--_opt := '{"a":"temp.f20170718fagebdcf580ac83_nsi_tit_tioc_adjust","b":"","subfix":"_diff", "schema":"temp"}';
--_opt := '{"a":"uploads.f20190507gfcdeba00508ed1","b":"coberturas.predios_pop","subfix":"_diff", "schema":"temp"}';
/*
_opt :=    json_build_object(
     	'a', 'coberturas.pdm',
        'condition_a', 'a.sicob_id=15711',
        'b', 'coberturas.plus',
        'add_sup_total', TRUE,
        'add_diff', TRUE,
        'filter_overlap', FALSE,
        'min_sup', 1
    );
*/

WITH
_params AS (
SELECT 
    COALESCE((opt->>'a')::text,'') as a,
    (sicob_split_table_name((opt->>'a')::text)).table_name as tbl_a,
    COALESCE((opt->>'condition_a')::text,'TRUE') as condition_a,
    COALESCE((opt->>'b')::text,'') as b,
    COALESCE((opt->>'condition_b')::text,'TRUE') as condition_b,
    (sicob_split_table_name((opt->>'b')::text)).table_name as tbl_b,
    COALESCE((opt->>'temp')::boolean,FALSE) as _temp,
    COALESCE((opt->>'subfix')::text,'_diff') as subfix,
    COALESCE((opt->>'schema')::text,'temp') as _schema,
    COALESCE((opt->>'add_geoinfo')::boolean,FALSE) as add_geoinfo,
    COALESCE((opt->>'add_fields')::boolean,FALSE) as add_fields,
    COALESCE((opt->>'min_sup')::real,0::real) as min_sup,
    COALESCE((opt->>'add_sup_total')::boolean,FALSE) as add_sup_total,
    COALESCE((opt->>'filter_overlap')::boolean,TRUE) as filter_overlap
 FROM (
 	SELECT 
    -- USAR PARA PRUEBAS
    /*
    json_build_object(
     	'a', 'coberturas.pdm',
        'condition_a', 'a.sicob_id=15711',
        'b', 'coberturas.plus',
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
b_inter_a AS (
	SELECT
    sicob_intersection(
    	json_build_object(
          'a', a,
          'condition_a',condition_a,
          'b',b,
          'condition_b',condition_b,
          'temp', true,
          'add_sup', false     
        )
    ) as res
    FROM
    _params 
),
b_inter_a_diss AS (
	SELECT 
    sicob_executesql('
      SELECT st_union(the_geom) as the_geom 
      FROM ' || (b_inter_a.res->>'lyr_intersected'),
      json_build_object(
        'table_out', 't' || MD5(random()::text),
        'temp', true,
        'create_index', true
      )
    ) AS res_inter
    FROM
    _params, b_inter_a
),
a_diff_b AS (
  SELECT 
  sicob_executesql('
  	SELECT * FROM (
      SELECT
          a.sicob_id as id_a, 
          st_difference(a.the_geom, ( SELECT the_geom FROM ' || (SELECT res_inter->>'table_out' FROM b_inter_a_diss) || ' limit 1) ) as the_geom
      FROM 
      ( 
        SELECT 
        sicob_id, (st_dump(the_geom)).geom as the_geom
        FROM ' || a || ' a
        WHERE
      	' || condition_a || '
      ) a     
    )t
    WHERE
    ST_IsEmpty(t.the_geom) = FALSE',
    json_build_object(
        'table_out', 't' || MD5(random()::text),
        'temp', true,
        'create_index', true
    )
  ) as res
  FROM
  _params
),
a_diff_b_fixsliver AS (
	SELECT
    sicob_executesql('
      SELECT 
        row_number() over() as sicob_id, 
        id_a, 
        CAST(NULL AS integer) as id_b, 
        ''' || a || ''' AS source_a,
        ''' || b || ''' AS source_b, 
        SICOB_utmzone_wgs84(the_geom) as sicob_utm, 
        round((ST_Area(ST_Transform(the_geom, SICOB_utmzone_wgs84(the_geom)))/10000)::numeric,5) as sicob_sup,
        the_geom
      FROM (
        SELECT 
        *
        FROM ( 
        	SELECT 
            id_a, (st_dump(the_geom)).geom as the_geom 
            FROM ' || (SELECT res->>'table_out' FROM a_diff_b) || '
        ) t 
        WHERE 
          trunc(
            st_area(
                st_buffer( 
                  st_buffer( the_geom ,-0.000001,''endcap=flat join=round quad_segs=1''), --> umbral de 0.5cm
                  0.000001,''endcap=flat join=round quad_segs=1''
                )
            )*10000000000
          ) > 0        
      ) t
      WHERE 
        ST_IsEmpty(t.the_geom) = FALSE
      ORDER BY t.id_a    
      ',
      json_build_object(
          'table_out', CASE WHEN _temp THEN 't' || MD5(random()::text) ELSE _schema || '.' || tbl_a || subfix || '_fixsliver' END,
          'temp', _temp,
          'create_index', true
      )
    ) as res
    FROM
    _params
),
a_diff_b_fields AS (
  SELECT
  sicob_executesql('
  	SELECT
      diff.sicob_id,' || 
      sicob_no_geo_column(
          a,
          '{id_a,id_b,sicob_id,sicob_utm,sicob_sup, topogeom}',
          'a.'
        ) || ', diff.sicob_sup, diff.sicob_utm, diff.the_geom
    FROM
    ' || a || ' a
    INNER JOIN 
    ' || (a_diff_b_fixsliver.res->>'table_out') || ' diff
    ON (
     a.sicob_id = diff.id_a
  	)',
    json_build_object(
      'table_out', CASE WHEN _temp THEN 't' || MD5(random()::text) ELSE _schema || '.' || tbl_a || subfix || '_fields' END,
      'temp', _temp,
      'create_index', true
    ) 
  ) as res
  FROM
  _params, a_diff_b_fixsliver
),
result AS (
  SELECT
	CASE WHEN add_fields THEN
    	(SELECT res->>'table_out' FROM a_diff_b_fields)
    ELSE
    	(SELECT res->>'table_out' FROM a_diff_b_fixsliver)
    END as lyr_difference,
    (SELECT res->>'row_cnt' FROM a_diff_b_fixsliver) as features_diff_cnt,
	CASE WHEN add_sup_total THEN
    	
        sicob_executesql('
          SELECT
              sum(sicob_sup) as sup
          FROM ' ||
          (SELECT res->>'table_out' FROM a_diff_b_fixsliver),
          json_build_object(
          	'return_scalar', true
          )
    	)->>'sup'
        
    ELSE
    	null
    END as diff_sup
  FROM _params
)
SELECT json_strip_nulls(row_to_json(t)) FROM result t INTO _out;
--SELECT * FROM a_diff_b_fixsliver

  
RETURN _out;

/** FIN!!! **/


EXCEPTION
WHEN others THEN
            RAISE EXCEPTION 'geoSICOB (sicob_difference) % , % , _opt: % ', SQLERRM, SQLSTATE, _opt;	
END;
$function$
;CREATE OR REPLACE FUNCTION public.sicob_difference(geometry, geometry)
 RETURNS geometry
 LANGUAGE plpgsql
AS $function$
DECLARE g geometry;
BEGIN
g :=  ST_Union(COALESCE(ST_Difference($1, $2), $1)) ;
RETURN g;
END;
$function$
;CREATE OR REPLACE FUNCTION public.sicob_difference(geom1 geometry, geom2 geometry, geom_type text)
 RETURNS geometry
 LANGUAGE plpgsql
 STABLE
AS $function$
DECLARE 
  diff geometry;
  exterior_ring geometry;
  sql text;
  diff_type text;
  geom_code integer;
  alternate_type text;
BEGIN
/*
 geom1 := 'MULTIPOLYGON(((-60.417921278527 -16.8530883325313,-60.4189945220582 -16.8544373819229,-60.4194515960511 -16.8544771738476,-60.4205026544912 -16.8545686731075,-60.4207894910172 -16.854004721193,-60.4207323211207 -16.8536455434266,-60.4203915579186 -16.8536113858879,-60.4198671330623 -16.8533956888677,-60.419839764618 -16.8531265006331,-60.4198144222014 -16.8527079801542,-60.4196352196733 -16.8521979093471,-60.4190165384765 -16.8520706385492,-60.4182371079817 -16.8523595438002,-60.417956004815 -16.8530082007488,-60.417921278527 -16.8530883325313)))'::geometry;
 geom2 := 'POLYGON((-60.4189954804597 -16.8544374653594,-60.4194515960511 -16.8544771738476,-60.4205026544912 -16.8545686731075,-60.4207894910172 -16.854004721193,-60.4207323211207 -16.8536455434266,-60.4203915579186 -16.8536113858879,-60.4198671330623 -16.8533956888677,-60.419839764618 -16.8531265006331,-60.4198144222014 -16.8527079801542,-60.4196352196733 -16.8521979093471,-60.4190165384765 -16.8520706385492,-60.4182371079817 -16.8523595438002,-60.417956004815 -16.8530082007488,-60.4179215281215 -16.8530877565857,-60.4189954804597 -16.8544374653594))'::geometry;
 geom_type := 'POLYGON';
 */

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

--> CONVIRTIENDO GEOMETRYCOLLECTION
 SELECT geometrytype(geom2) INTO diff_type;
 IF diff_type = 'GEOMETRYCOLLECTION' then
 	SELECT st_makevalid(ST_CollectionExtract(geom2,geom_code)) INTO geom2;
 end if;
 IF diff_type = 'MULTIPOLYGON' AND st_isvalid(geom2)=FALSE THEN
 	SELECT st_makevalid(geom2) INTO geom2;
 end if;

/*
SELECT st_union(t.the_geom) 
FROM (
	SELECT st_exteriorring(the_geom) as the_geom
    FROM (
    	SELECT (st_dump(geom1)).geom as the_geom
    ) u
) t INTO exterior_ring;
 */
 
--SELECT  st_exteriorring(geom1) INTO exterior_ring;


SELECT  ST_Collect(ST_ExteriorRing(the_geom))  INTO  exterior_ring
	FROM (
        SELECT (dp).path[1] as gid, (dp).geom as the_geom 
        FROM(
            SELECT st_dump(geom1) as dp
            ) t        
    ) As u
GROUP BY gid;
 
SELECT   ST_Difference(geom1,geom2) INTO diff;
/*
 SELECT   ST_Difference( ST_Difference(geom1,geom2),  st_buffer( exterior_ring,0.00000001,'join=mitre') ) INTO diff;


SELECT
st_snap(
	diff,
	exterior_ring,
    0.0000001
    --0.00000000001
) INTO diff;
*/

 --SELECT ST_Difference(geom1,geom2) INTO diff;
 
 SELECT geometrytype(diff) INTO diff_type;
 
 if diff_type = 'GEOMETRYCOLLECTION' then
  select ST_CollectionExtract(diff,geom_code) INTO diff;
  SELECT geometrytype(diff) INTO diff_type;
 end if;

 IF diff_type in (geom_type,alternate_type) then	
  return diff;
 else
  return null;
 end if;
 
EXCEPTION
WHEN others THEN
	RAISE EXCEPTION 'geoSICOB (sicob_difference)-> geom1:% | geom2:% | geom_type:% >> % (%), diff --> %, SQL --> %', st_astext(geom1) , st_astext(geom2),  geom_type,  SQLERRM, SQLSTATE, st_astext(diff), sql;	
            
end;
$function$
 