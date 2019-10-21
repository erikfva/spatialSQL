SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_overlap2x(_opt json)
 RETURNS json
 LANGUAGE plpgsql
AS $function$
DECLARE

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
--> filter_overlap (opcional): Indica si se filtraran poligonos solapados en la capa resultado. Por defecto es TRUE.
--> add_sup_total (opcional true/false): Calcula y devuelve la superficie total sobrepuesta en hectareas.
--> min_sup (opcional): Superficie minima (en hectareas) permitida para los poligonos de la capa resultado.
--> add_geoinfo (opcional true/false): Agrega o no la informaciï¿½n del cï¿½digo de proyecciï¿½n UTM 'sicob_utm', superficie (ha) y 'sicob_sup' para cada polï¿½gono.
--> add_fields (opcional true/false): Agrega los campos de "b" en el resultado.
--> add_sup (opcional true/false): Calcular la superficie en ha. Por defecto es "true".
--> add_diff (opcional true/false): Agrega o no los poligono que no se intersectan a la  capa resultado. Se asigna "NULL" a los campos de atributos para esos poligonos. 

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
    COALESCE((opt->>'add_sup_total')::boolean,FALSE) as add_sup_total,
    COALESCE((opt->>'add_diff')::boolean,FALSE) as add_diff
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
overlayer AS (
	SELECT
	sicob_executesql('
    with
    a AS (
        SELECT
          row_number() OVER() as gid,
          sicob_id,
          the_geom
        FROM (
          SELECT
          sicob_id,
          (st_dump(the_geom)).geom as the_geom 
          FROM
          ' || a || ' a
          WHERE
          (' || condition_a || ')
        ) t
    ),
    b AS (
        SELECT
            sicob_id,
            (st_dump(the_geom)).geom as the_geom 
        FROM (   
          SELECT
          b.sicob_id,
           ST_UnaryUnion( b.the_geom) as the_geom 
          FROM
          ' || b || ' 
          b
          join  a  
          t2 on st_intersects(t2.the_geom, b.the_geom)
          WHERE
          ( ' || condition_b || ' )
          group by b.sicob_id
        ) t
    ),
    a_inter_b AS (
		SELECT
          id_a,
          id_b,
          the_geom          
        FROM (
          SELECT
          id_a,
          id_b, 
          (st_dump(the_geom)).geom as the_geom
          FROM (
              select 
                  a.sicob_id as id_a,
                  b.sicob_id as id_b,
                  st_intersection(a.the_geom,b.the_geom) as the_geom
              from
                  a join  b on ( st_intersects(a.the_geom,b.the_geom) AND NOT ST_Touches(a.the_geom, b.the_geom) )
          ) t1
        ) t
            WHERE 
              trunc(
                st_area(
                	the_geom
                /*
                    st_buffer( 
                      st_buffer( t.the_geom ,-0.000001,''endcap=flat join=round quad_segs=1''), --> umbral de 0.5cm
                      0.000001,''endcap=flat join=round quad_segs=1''
                    )
                */
                )*10000000000
              ) > 0
    ),
    a_inter_b_fields AS (
      SELECT
        i.id_a,
        i.id_b,
        ' || sicob_no_geo_column(b,'{sicob_id,id_a,source_a,id_b,source_b, sicob_sup, sicob_utm}','b.') || ',
        i.the_geom
      FROM
        a_inter_b i
        INNER JOIN ' || b || ' 
        b
        ON ( b.sicob_id = i.id_b)
    ),
    a_inter_b_diss_group AS (
        SELECT
            id_a as sicob_id,
            ST_union(the_geom) as the_geom
        FROM
            a_inter_b 
        group by (id_a)
    ),
    a_inter_b_consolid AS (
        SELECT
        a.sicob_id,
        a.the_geom,
        c.the_geom as the_geom_inter
        FROM
        ' || a || ' a join a_inter_b_diss_group c
        on (a.sicob_id = c.sicob_id)
    ),
    a_diff_inter_b AS (
        SELECT
            sicob_id,
            --sicob_normalize_geometry(t.the_geom, 0.5, 0.5, 0.005, 0.0001) as
            
            st_buffer( 
                st_buffer( t.the_geom ,-0.0000001,''join=mitre''), --> umbral de 0.5cm
                0.0000001,''join=mitre''
            ) as 
            
            the_geom
        FROM (
          SELECT
            sicob_id,
            (st_dump(the_geom)).geom as the_geom 
          FROM (
            SELECT 
              a.sicob_id,
              st_difference(a.the_geom,
               a.the_geom_inter  
              ) as the_geom 
            FROM 
            	a_inter_b_consolid a
            WHERE
              trunc(st_area(a.the_geom) * 10000000) <>
              trunc(st_area(a.the_geom_inter) * 10000000)
          ) t1
        ) t
        WHERE
        	trunc(
                st_area(
                	t.the_geom
                )*10000000000
              ) > 0   
    ),
    a_diff_b AS (
        SELECT
            sicob_id,
            the_geom
        FROM
            a
        WHERE
            sicob_id NOT IN (
                SELECT DISTINCT
                id_a
                FROM
                a_inter_b
            )
        UNION ALL
        SELECT
            sicob_id,
            the_geom
        FROM
        a_diff_inter_b
        WHERE
              trunc(
                st_area(
                    the_geom
                )*10000000000
              ) > 0 
    ),
    overlayer AS (
    	SELECT
        	row_number() over() as sicob_id,
            id_a,
            id_b,
            the_geom
        FROM (           
          SELECT
            id_a,
            id_b,
            the_geom
          FROM
          a_inter_b
          UNION ALL
          SELECT
            sicob_id as id_a,
            NULL as id_b,
            the_geom
          FROM
          a_diff_b  
        ) t  
    )
      SELECT 
      	t.*, ' ||
        '''' || a::text || ''' as source_a, ''' || b::text || ''' as source_b' ||
        CASE WHEN add_sup OR add_sup_total OR min_sup > 0 THEN
        ', ' ||
        'SICOB_utmzone_wgs84(t.the_geom) as sicob_utm, ' ||
        'round((ST_Area(ST_Transform(t.the_geom, SICOB_utmzone_wgs84(t.the_geom)))/10000)::numeric,5) as sicob_sup '
        ELSE
        	''
        END || '
      FROM 
      ' ||
      	CASE WHEN add_diff THEN
        	'overlayer'
        ELSE
        	'a_inter_b'
        END
       || ' t'
      ,
      json_build_object(
            'table_out', CASE WHEN _temp THEN 't' || MD5(random()::text) ELSE _schema || '.' || tbl_a || subfix END ,
            'temp', _temp,
            'create_index', true
      )
	)->>'table_out' as lyr
    FROM
    _params
),
overlayer_fields AS (
	SELECT
      sicob_executesql('
        SELECT
        	o.sicob_id,
            o.id_a,
            o.id_b,
            o.source_a,
            o.source_b,
            ' || sicob_no_geo_column(p.b,'{sicob_id,id_a,source_a,id_b,source_b, sicob_sup, sicob_utm}','b.') || ',
            o.the_geom
        FROM
            ' || o.lyr || ' o
            left JOIN
            ' || p.b || ' b
            on (o.id_b = b.sicob_id)
        ',
        json_build_object(
              'table_out', CASE WHEN p._temp THEN 't' || MD5(random()::text) ELSE p._schema || '.' || p.tbl_a || p.subfix || '_fields' END ,
              'temp', p._temp,
              'create_index', true
        )
      )->>'table_out' as lyr
      FROM
      overlayer o, _params p
),
overlayer_info AS (
	SELECT
		CASE WHEN p.add_fields THEN 
          (SELECT lyr FROM overlayer_fields) 
        ELSE 
          o.lyr 
        END as lyr,
        COALESCE(
            (sicob_executesql('
            SELECT 
                sum(sicob_sup) as total 
            FROM ' || o.lyr || '
            WHERE id_b IS NOT NULL' ,
            json_build_object('return_scalar',TRUE)
            )->>'total')::float
          , 0
        ) as inters_sup,
        CASE WHEN p.add_diff THEN
        	COALESCE(
            	(sicob_executesql('
                SELECT 
                    sum(sicob_sup) as total 
                FROM ' || o.lyr || '
                WHERE id_b IS NULL' ,
                json_build_object('return_scalar',TRUE)
              	)->>'total')::float
              , 0
            )
        ELSE
            null
        END as diff_sup
    FROM
    overlayer o, _params p
),
a_inter_b AS (
	SELECT
    	sicob_executesql('
          SELECT
              *
          FROM ' || 
          oi.lyr || '
          WHERE
              id_b IS NOT NULL
          ',
          json_build_object(
              'table_out', CASE WHEN p._temp THEN 't' || MD5(random()::text) ELSE p._schema || '.' || p.tbl_a || p.subfix || '_inter' END ,
              'temp', p._temp,
              'create_index', true
          )
        ) as res
    FROM
    	overlayer_info oi, _params p
),
a_inter_b_diss AS (
    SELECT  
      sicob_dissolve(
      	json_build_object(
        	'lyr_in', res->'table_out',
            'add_sup', TRUE
        )
      ) as res
    FROM
      a_inter_b
),
a_diff_b AS (
	SELECT
    	sicob_executesql('
          SELECT
              *
          FROM ' || 
          oi.lyr || '
          WHERE
              id_b IS NULL
          ',
          json_build_object(
              'table_out', CASE WHEN p._temp THEN 't' || MD5(random()::text) ELSE p._schema || '.' || p.tbl_a || p.subfix || '_diff' END ,
              'temp', p._temp,
              'create_index', true
          )
        ) as res
    FROM
    	overlayer_info oi, _params p
),
a_diff_b_diss AS (
    SELECT  
      sicob_dissolve(
      	json_build_object(
        	'lyr_in', res->'table_out',
            'add_sup', TRUE
        )
      ) as res
    FROM
      a_diff_b
),
result AS (
  SELECT 
      oi.lyr as lyr_over,
      inter.res->'table_out' as lyr_intersected,
      inter.res->'row_cnt' as features_inters_cnt,
      oi.inters_sup,
      
      CASE WHEN p.add_diff THEN
      	(SELECT res->'table_out' FROM a_diff_b)
      ELSE
      	NULL
      END as lyr_difference,
      CASE WHEN p.add_diff THEN
      	(SELECT res->'row_cnt' FROM a_diff_b)
      ELSE
      	NULL
      END as features_diff_cnt,
      oi.diff_sup,
      
      CASE WHEN p.add_sup_total THEN
      	COALESCE(oi.diff_sup,0) + COALESCE(oi.inters_sup,0)
      ELSE
      	NULL
      END as sicob_sup_total
  FROM overlayer_info oi, a_inter_b inter, _params p
)
SELECT json_strip_nulls(row_to_json(t)) FROM result t INTO _out;
  
    RETURN _out;
    

EXCEPTION
WHEN others THEN
            RAISE EXCEPTION 'geoSICOB (sicob_overlap2x) % , % , _opt: % ', SQLERRM, SQLSTATE, _opt;	
END;
$function$
 