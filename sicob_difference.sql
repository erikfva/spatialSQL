SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_difference(_opt json)
 RETURNS json
 LANGUAGE plpgsql
AS $function$
DECLARE 
  a TEXT;
  b TEXT;
  _condition_a text; _condition_b text;
  _subfixresult text;
  _schema text;
  _tolerance double precision;

---------------------------------
  
 sql text;

 tbl_nameA text; sch_nameA text;
 tbl_nameB text; sch_nameB text;
 
 row_cnt integer;
 __a text; __b text; a__b text;
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

---------------------------
--VALORES DEVUELTOS
---------------------------
--> lyr_difference : capa resultante con elementos de "a" que no se intersectan con "b". Solo se incluyen dos campos: sicob_id,the_geom.

--_opt := '{"a":"temp.f20170718fagebdcf580ac83_nsi_tit_tioc_adjust","b":"","subfix":"_diff", "schema":"temp"}';
--_opt := '{"a":"uploads.f20190507gfcdeba00508ed1","b":"coberturas.predios_pop","subfix":"_diff", "schema":"temp"}';

a := (_opt->>'a')::TEXT;
_condition_a := COALESCE((_opt->>'condition_a')::text, 'TRUE');

b := COALESCE((_opt->>'b')::TEXT, '');
_condition_b := COALESCE((_opt->>'condition_b')::text, 'TRUE');

_subfixresult := COALESCE(_opt->>'subfix','') || '_diff'; 
_schema := COALESCE(_opt->>'schema','temp');


SELECT * FROM sicob_split_table_name(a::text) INTO sch_nameA, tbl_nameA;
SELECT * FROM sicob_split_table_name(b::text) INTO sch_nameB, tbl_nameB;

row_cnt := 0;
sql := '';

IF b <> '' THEN
	sql := '      
    select  
    	distinct b.sicob_id as id_b, 
        ''' || a || ''' AS source_a, 
        ''' || b || ''' AS source_b, b.the_geom
    from 
      ' || a || ' a 
      INNER JOIN ' || b || ' b
      ON (' || _condition_a || ' AND ' || _condition_b || ' AND st_intersects(a.the_geom, b.the_geom) AND NOT ST_Touches(a.the_geom, b.the_geom))';
	_out := sicob_executesql( 
            	sql,
            	json_build_object(
                      'table_out', a || '_i_b',
                      'temp', true,
                      'create_index', true
                )
			);
    sql := '';
END IF;

--> Caso: Si B es conjunto vacio o no existe interseccion de A con B
--	se debe devolver a
IF b = '' or COALESCE((_out->>'row_cnt')::integer, 1) = 0 THEN --> Si b es un conjunto vacio.   
    --> No existe interseccion. Retornar todos los poligonos de "a".
        sql := '
        SELECT 
        	a.sicob_id, a.sicob_id as id_a,
            ''' || a || ''' AS source_a,
            ''' || b || ''' AS source_b,
            SICOB_utmzone_wgs84(the_geom) as sicob_utm,
            st_area(ST_Transform(the_geom, SICOB_utmzone_wgs84(the_geom) ))/10000 as sicob_sup,
        	a.the_geom 
		FROM ' || a || ' a WHERE ' || _condition_a;
END IF;

IF sql = '' THEN
    sql := '
      WITH 
      b_inter_a_diss AS (
          SELECT st_union(the_geom) as the_geom 
          FROM ' || (_out->>'table_out')::text || '
      ),
      a_diff_b AS (
          SELECT 
              a.sicob_id as id_a, st_difference(a.the_geom, ( SELECT the_geom FROM b_inter_a_diss limit 1) ) as the_geom
          FROM 
          ' || a || ' a
      ),
      a_diff_b_fixsliver AS (
          SELECT *
          FROM
          ( SELECT id_a, (st_dump(the_geom)).geom as the_geom FROM a_diff_b) t 
          WHERE 
            trunc(
              st_area(
                  st_buffer( 
                    st_buffer( the_geom ,-0.000001,''endcap=flat join=round quad_segs=1''), --> umbral de 0.5cm
                    0.000001,''endcap=flat join=round quad_segs=1''
                  )
              )*10000000000
            ) > 0
      /*
          SELECT 
          id_a,
            st_buffer( 
                  st_buffer( the_geom ,-0.0000001,''endcap=flat join=round quad_segs=1''), --> umbral de 0.5cm
                  0.0000001,''endcap=flat join=round quad_segs=1''
           ) as the_geom
          FROM 
          ( SELECT id_a, (st_dump(the_geom)).geom as the_geom FROM a_diff_b) t 
          WHERE 
          trunc(st_area(the_geom)*10000000000) > 0
      */
      )
      SELECT 
        row_number() over() as sicob_id, 
        id_a, 
        NULL::integer as id_b, 
        ''' || a || ''' AS source_a,
        ''' || b || ''' AS source_b, 
        SICOB_utmzone_wgs84(the_geom) as sicob_utm, 
        round((ST_Area(ST_Transform(the_geom, SICOB_utmzone_wgs84(the_geom)))/10000)::numeric,5) as sicob_sup,
        the_geom
      FROM 
        a_diff_b_fixsliver
      WHERE 
        ST_IsEmpty(the_geom) = FALSE
      ORDER BY id_a
    ';
END IF;

IF COALESCE((_opt->>'join_columns')::boolean, FALSE) THEN
	__a := tbl_nameA || '__diff';
    EXECUTE 'DROP TABLE IF EXISTS ' || __a;
    EXECUTE 'CREATE TEMPORARY TABLE ' || __a || ' ON COMMIT DROP AS ' || sql;
    sql := '
        SELECT
            diff.sicob_id,' || 
            sicob_no_geo_column(
                a,
                '{sicob_id,sicob_utm,sicob_sup, topogeom}',
                'a.'
            ) || ', diff.sicob_sup, diff.sicob_utm, diff.the_geom
        FROM
        ' || a || ' a
        INNER JOIN 
        ' || __a || ' diff
        ON (
         a.sicob_id = diff.id_a
        )
    ';
END IF;


__a := tbl_nameA || _subfixresult;
IF COALESCE((_opt->>'temp')::boolean, FALSE) THEN
    EXECUTE 'DROP TABLE IF EXISTS ' || __a;
    EXECUTE 'CREATE TEMPORARY TABLE ' || __a || ' ON COMMIT DROP AS ' || sql;
ELSE
    __a := _schema || '.' || __a;
    EXECUTE 'DROP TABLE IF EXISTS ' || __a;
    EXECUTE 'CREATE UNLOGGED TABLE ' || __a || ' AS ' || sql;
END IF;
GET DIAGNOSTICS row_cnt = ROW_COUNT; -->obteniendo la cantidad de registros creados.
EXECUTE 'ALTER TABLE ' || __a || ' ADD PRIMARY KEY (sicob_id);';

_out := ('{"lyr_difference":"' || __a || '","features_diff_cnt":"' || row_cnt || '"}')::json;

--CALCULANDO LA SUPERFICIE TOTAL RESULTANTE en HA.
IF COALESCE((_opt->>'add_sup_total')::boolean, FALSE) THEN
    IF row_cnt > 0 THEN
        sql := '
            SELECT 
            sum(st_area(ST_Transform(the_geom, SICOB_utmzone_wgs84(the_geom) ))/10000) as sicob_sup
            FROM ' || __a;
        EXECUTE sql INTO sicob_sup_total;
    ELSE
        sicob_sup_total := 0;
    END IF;
    _out := _out::jsonb || jsonb_build_object('diff_sup',sicob_sup_total);
END IF;
  
RETURN _out;

/** FIN!!! **/


EXCEPTION
WHEN others THEN
            RAISE EXCEPTION 'geoSICOB (sicob_difference) % , % , _opt: % | sql: %', SQLERRM, SQLSTATE, _opt, sql;	
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