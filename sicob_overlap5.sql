CREATE OR REPLACE FUNCTION public.sicob_overlap5(_opt json)
 RETURNS json
 LANGUAGE sql
 STABLE
AS $function$
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
--> sicob_sup_total : superficie total sobrepuesta en hectareas. (si se indicó cualquiera de los parámetro "add_sup_total/min_sup/add_geoinfo")

WITH
_params AS (
SELECT 
	--'processed.f20171005fgcbdae84fb9ea1_nsi'::text as a,
    COALESCE((_opt->>'a')::text,'') as a,
    COALESCE((_opt->>'condition_a')::text,'TRUE') as condition_a,
    COALESCE((_opt->>'b')::text,'') as b,
    COALESCE((_opt->>'condition_b')::text,'TRUE') as condition_b,
	COALESCE((_opt->>'tolerance')::text,'0') as tolerance,
    COALESCE((_opt->>'temp')::boolean,FALSE) as _temp,
    COALESCE((_opt->>'subfix')::text,'_overlap') as subfix,
    COALESCE((_opt->>'schema')::text,'temp') as _schema,
    COALESCE((_opt->>'add_geoinfo')::boolean,FALSE) as add_geoinfo,
    COALESCE((_opt->>'add_diff')::boolean,FALSE) as add_diff,
    COALESCE((_opt->>'min_sup')::real,0::real) as min_sup,
    COALESCE((_opt->>'add_sup_total')::boolean,FALSE) as add_sup_total,
    COALESCE((_opt->>'filter_overlap')::boolean,TRUE) as filter_overlap
),
_a_filtered AS (
	SELECT
    	CASE WHEN condition_a = 'TRUE' THEN
        	a::text 
        ELSE
			sicob_executesql( 
            	'SELECT sicob_id, the_geom FROM ' || a || ' a WHERE ' || condition_a,
            	json_build_object(
                      'table_out', a || '_filtered',
                      'temp', true,
                      'create_index', true
                )
			)->>'table_out'        
        END as table_name 
    FROM
    _params
),
_adjusted AS (
	SELECT 
    	CASE WHEN tolerance::real > 0 THEN
        	sicob_executesql(
                '
                SELECT 
                    sicob_id,
                    CASE WHEN ST_NRings( the_geom ) > 1 THEN
                        sicob_update_exteriorring( 
                            sicob_snap_edge(
                                st_exteriorring(the_geom),''' || 
                                json_build_object(
                                          'target', b,
                                          'condition',condition_b,
                                          'tolerance',tolerance, 
                                          'returnpolygon',false
                                ) || '''::json
                            ), 
                            the_geom, 
                            ''{}''
                        )
                    ELSE 
                        sicob_snap_edge(
                            st_exteriorring(the_geom),''' || 
                            json_build_object(
                              'target', b,
                              'condition',condition_b,
                              'tolerance',tolerance, 
                              'returnpolygon',true
                            ) || '''::json                    
                        )                    
                    END as the_geom
                FROM ' || (SELECT table_name FROM _a_filtered LIMIT 1),        
                json_build_object(
                  'table_out', a || '_adjust',
                  'temp', true,
                  'create_index', true
                )
            )->>'table_out'
    	ELSE
			(SELECT table_name FROM _a_filtered LIMIT 1)
     	END
    	as lyr_adjusted
    FROM _params
),
_intersected AS (
    SELECT 
    sicob_intersection(
        json_build_object(
            'a',(SELECT lyr_adjusted FROM _adjusted LIMIT 1),
            'b',b,
            'condition_b',condition_b,
            'schema', _schema,
            'filter_overlap',filter_overlap
        )
    ) as res_inter
    FROM _params limit 1
),
_difference AS (
    SELECT 
    sicob_difference(
        json_build_object(
            'a',(SELECT lyr_adjusted FROM _adjusted LIMIT 1),
            'b',(SELECT res_inter->>'lyr_intersected' FROM _intersected LIMIT 1),
            'schema', _schema
        )
    ) as res_diff
    FROM _params limit 1
),
_buildoverlap AS (
	SELECT
        CASE WHEN r.lyr_difference <> '' OR r.lyr_intersected <> '' THEN -->Si existen datos
            sicob_executesql(
                CASE WHEN r.lyr_intersected <> '' THEN
                    '(SELECT id_a, id_b, the_geom FROM ' || r.lyr_intersected || ')'
                ELSE
                    ''::text
                END ||
                CASE WHEN r.lyr_difference <> '' AND r.lyr_intersected <> '' THEN
                  ' UNION ALL '
                ELSE
                    ''::text
                END ||
                CASE WHEN r.lyr_difference <> '' THEN
                    '(SELECT id_a, null::integer as id_b, the_geom FROM ' || r.lyr_difference || ')'
                ELSE
                    ''::text
                END ,
                json_build_object(
                      'table_out',  (SELECT table_name || '___overlap' FROM sicob_split_table_name(r.opt->>'a') limit 1) ,
                      'temp', true,
                      'add_geoinfo', (r.opt->>'add_geoinfo')::boolean OR (r.opt->>'add_sup_total')::boolean OR (r.opt->>'min_sup')::real > 0
                )
            )
		ELSE
        	'{"table_out":"","row_cnt":"0"}'::json
        END as res_buildoverlap 
    FROM (
    SELECT
      (SELECT res_inter->>'lyr_intersected' FROM _intersected) as lyr_intersected,
      CASE WHEN (SELECT add_diff FROM _params) THEN
      	(SELECT res_diff->>'lyr_difference' FROM _difference)
      ELSE
        ''::text 
      END as lyr_difference,
      (SELECT row_to_json(t) FROM _params t) as opt
    ) r
),
_overlap as (
	SELECT
    	CASE WHEN r.res_overlap->>'table_out' <> '' THEN -->Si existen datos
            sicob_executesql(
                'SELECT row_number() over() as sicob_id, ''' || 
                (r.opt->>'a')::text || '''::text as source_a, ''' || 
                (r.opt->>'b')::text || '''::text as source_b,' || 
                columns_b || ', t.*
                FROM(
                    SELECT * FROM ' ||
                    (r.res_overlap->>'table_out'::text) || 
                    CASE WHEN (r.opt->>'min_sup'::text)::real > 0 THEN ' WHERE sicob_sup >= ' || (r.opt->>'min_sup'::text)::text ELSE '' END ||
                    ' ORDER BY id_a, id_b 
                ) t LEFT JOIN ' || (r.opt->>'b') || ' b ON (b.sicob_id = t.id_b)
                ',
                json_build_object(
                  'table_out',  (SELECT COALESCE(r.opt->>'_schema','temp') || '.' || table_name || COALESCE(r.opt->>'subfix','_overlap') FROM sicob_split_table_name(r.opt->>'a') limit 1) ,
                  'temp', r.opt->>'_temp',
                  'create_index', true
                )

            )
		ELSE
        	r.res_overlap
        END as res_overlap
    FROM(
      SELECT
    	(SELECT res_buildoverlap FROM _buildoverlap) as res_overlap,
    	(SELECT row_to_json(t) FROM _params t) as opt,
    	(SELECT sicob_no_geo_column(b,'{sicob_id,id_a,source_a,id_b,source_b, sicob_sup, sicob_utm}','b.') FROM _params) as columns_b
    )r
),
_statistics as (
	SELECT
    	CASE WHEN r.res_overlap->>'table_out' <> '' THEN -->Si existen datos
          (sicob_executesql(
          '
          SELECT
          (
            SELECT row_to_json(t)::jsonb
            FROM
            (
              SELECT count(sicob_id) as features_inters_cnt' || 
              CASE WHEN (r.opt->>'add_geoinfo')::boolean OR (r.opt->>'add_sup_total')::boolean OR (r.opt->>'min_sup')::real > 0 THEN ', sum(sicob_sup) as inters_sup' ELSE '' END || 
              ' FROM 
                  ' || (r.res_overlap->>'table_out')::text || '
              WHERE 
                  id_b IS NOT NULL
            )t
          ) ||
          (
            SELECT row_to_json(t)::jsonb
            FROM (
              SELECT count(sicob_id) as features_diff_cnt' || 
              CASE WHEN (r.opt->>'add_geoinfo')::boolean OR (r.opt->>'add_sup_total')::boolean OR (r.opt->>'min_sup')::real > 0 THEN ', sum(sicob_sup) as diff_sup' ELSE '' END || 
              ' FROM 
                  ' || (r.res_overlap->>'table_out')::text || '
              WHERE 
                  id_b IS NULL
            )t
          )  as summary    
          ',
          json_build_object('return_scalar',true)
          )->>'summary')::json 
        ELSE
        	'{}'::json
        END as summary 
 	FROM(
 		SELECT
        	(SELECT row_to_json(t) FROM _params t) as opt,
            (SELECT res_overlap FROM _overlap) as res_overlap
	)r
)
SELECT
    jsonb_strip_nulls(
        json_build_object(
        'lyr_over' , r.res_overlap->>'table_out',
        'sicob_sup_total', COALESCE((r.summary->>'inters_sup')::real,0::real) + COALESCE((r.summary->>'diff_sup')::real, 0::REAL)
        )::jsonb || r.summary::jsonb
     )::json
 FROM(
 	SELECT
 		(SELECT res_overlap FROM _overlap) as res_overlap,
        (SELECT row_to_json(t) FROM _params t) as opt,
        (SELECT summary::json FROM _statistics) as summary
      
 ) r 
 
/*
(SELECT * FROM _intersected) as res_inter,
(SELECT * FROM _difference) as res_diff,
(SELECT * FROM _adjusted) as res_adjust
*/
$function$
 