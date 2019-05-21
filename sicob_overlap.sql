SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_overlap(_opt json)
 RETURNS jsonb
 LANGUAGE sql
 STABLE
AS $function$
---------------------------
--PARAMETROS DE ENTRADA
---------------------------
--> a : capa que se sobrepondrÃ¡.
--> condition_a (opcional): Filtro para los datos de "a". Si no se especifica, se toman todos los registros.
--> b : capa donde se sobrepondrÃ¡ la capa "a".
--> condition_b (opcional): Filtro para los datos de "b". Si no se especifica, se toman todos los registros.
--> subfix (opcional): texto adicional que se agregarÃ¡ al nombre de "a" para formar el nombre de la capa resultante. Si no se especifica por defecto es "_overlap".
--> schema (opcional): esquema del la BD donde se crearÃ¡ la capa resultante. Si no se especifica se crearÃ¡ en "processed".
--> tolerance (opcional): Distancia mÃ¡xima (en metros) para el autoajuste automÃ¡tico de los bordes de "a" hacia los bordes de "b" (snapping). Si no se especifica, no se realiza autoajuste. 
--> add_geoinfo (opcional true/false): Agrega o no la informaciÃ³n del cÃ³digo de proyecciÃ³n UTM 'sicob_utm', superficie (ha) 'sicob_sup' y la geometrÃ­a en proyecciÃ³n webmercator 'the_geom_webmercator' para cada polÃ­gono. Si no se especifica NO se agrega.
--> add_diff (opcional true/false): Agrega o no los polÃ­gono que no se intersectan a la  capa resultado. Se asigna "NULL" a los campos de atributos para esos polÃ­gonos. 
--> temp (opcional true/false) : Indica si la capa resultante serÃ¡ temporal mientras dura la transacciÃ³n. Esto se requiere cuando el resultado es utilizado como capa intermedia en otros procesos dentro de la misma transacciÃ³n.
--> add_sup_total (opcional true/false): Calcula y devuelve la superficie total sobrepuesta en hectareas.
--> min_sup (opcional): Superficie minima (en hectareas) permitida para los poligonos de la capa resultado, solo se aplica si se el parametro add_geoinfo es TRUE.
--> filter_overlap (opcional true/false) : Indica si se permite o no sobreposicion de poligonos en la capa resultado. Por defecto es TRUE.
---------------------------
--VALORES DEVUELTOS
---------------------------
--> lyr_over : capa resultante de la sobreposiciÃ³n.
--> features_inters_cnt : cantidad de poligonos que se intersectan.
--> features_diff_cnt : cantidad de poligonos que NO se intersectan.
--> sicob_sup_total : superficie total sobrepuesta en hectareas. (si se indicÃ³ cualquiera de los parÃ¡metro "add_sup_total/min_sup/add_geoinfo")


WITH
_params AS (
SELECT 
	--'processed.f20171005fgcbdae84fb9ea1_nsi'::text as a,
    COALESCE((opt->>'a')::text,'') as a,
    (SELECT table_name FROM sicob_split_table_name((opt->>'a')::text)) as tbl_a,
    COALESCE((opt->>'condition_a')::text,'TRUE') as condition_a,
    COALESCE((opt->>'b')::text,'') as b,
    COALESCE((opt->>'condition_b')::text,'TRUE') as condition_b,
	COALESCE((opt->>'tolerance')::text,'0') as tolerance,
    COALESCE((opt->>'temp')::boolean,FALSE) as _temp,
    COALESCE((opt->>'subfix')::text,'_overlap') as subfix,
    COALESCE((opt->>'schema')::text,'temp') as _schema,
    COALESCE((opt->>'add_geoinfo')::boolean,FALSE) as add_geoinfo,
    COALESCE((opt->>'add_diff')::boolean,FALSE) as add_diff,
    COALESCE((opt->>'min_sup')::real,0::real) as min_sup,
    COALESCE((opt->>'add_sup_total')::boolean,FALSE) as add_sup_total,
    COALESCE((opt->>'filter_overlap')::boolean,TRUE) as filter_overlap,
    -- CREANDO EL NOMBRE PARA LA TABLA DE SALIDA
    CASE WHEN COALESCE((opt->>'temp')::boolean,FALSE) THEN
    	''
    ELSE
    	COALESCE((opt->>'schema')::text,'temp') || '.'
    END ||
    (SELECT table_name FROM sicob_split_table_name((opt->>'a')::text)) ||
    COALESCE((opt->>'subfix')::text,'_overlap') as tbl_out
 FROM (
 	SELECT 
    -- USAR PARA PRUEBAS
    /*
    json_build_object(
     	'a', 'uploads.f20190515fgdcbead21d39cd',
        'b', 'coberturas.predios_titulados',
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
_intersected AS (
    SELECT 
    sicob_intersection(
        json_build_object(
            'a',a,
            'condition_a',condition_a,
            'b',b,
            'condition_b',condition_b,
            'schema', _schema,
            'temp', _temp, --TRUE,
            'subfix', subfix,
            'filter_overlap',filter_overlap,
            'add_sup_total',add_sup_total,
            'min_sup', min_sup,
            'add_geoinfo', add_geoinfo,
            'add_fields', TRUE
        )
    ) as res_inter
    FROM _params limit 1
),
_difference AS (
    SELECT
    CASE WHEN add_diff THEN 
        sicob_difference(
            json_build_object(
                'a',a,
            	'condition_a',condition_a,
                'b',b,
                'condition_b',condition_b,
                'schema', _schema,
                'temp',_temp,
                'add_geoinfo', add_geoinfo,
                'filter_overlap',filter_overlap,
                'add_sup_total',add_sup_total,
				'min_sup', min_sup        
            )
        )
	ELSE
    	'{}'::json 
    END as res_diff
    FROM _params limit 1
),
--CLONANDO lyr_intersected
_clone_intersected AS (
	SELECT 
    sicob_executesql('SELECT * FROM ' ||
    	(SELECT res_inter->>'lyr_intersected' FROM _intersected)::text,
        json_build_object(
        	'table_out', tbl_out,
            'temp', _temp,
            'create_index', TRUE
        )
    ) AS res_clone
    FROM
    _params
),
_merge_inter_diff AS (
	SELECT
	sicob_executesql('insert into ' || (SELECT res_clone->>'table_out' FROM _clone_intersected)::text || '
      (id_a, source_a, source_b, sicob_sup, the_geom)
      select 
       id_a, source_a, source_b, sicob_sup, the_geom
      from
      ' || (SELECT res_diff->>'lyr_difference' FROM _difference)::text ,  
      '{}'::json
	)->>'row_cnt' as row_cnt
)
--SELECT * FROM _merge
SELECT
	(SELECT * FROM _intersected)::jsonb || 
    CASE WHEN add_diff THEN
    	(SELECT * FROM _difference)::jsonb ||
        jsonb_build_object(
            'features_diff_cnt' , (SELECT row_cnt FROM _merge_inter_diff),
            'lyr_over', tbl_out
        )
    ELSE
        jsonb_build_object(
            'lyr_over', (SELECT res_inter->>'lyr_intersected' FROM _intersected)::text
        )
    END || 
    CASE WHEN add_sup_total THEN
    	jsonb_build_object(
        	'sicob_sup_total',
            (SELECT res_inter->>'inters_sup' FROM _intersected)::real +       
            CASE WHEN add_diff THEN
                (SELECT res_diff->>'diff_sup' FROM _difference)::real  
            ELSE
                0
            END          
        )
    ELSE
        '{}'::jsonb
    END
FROM
_params



/***********
WITH
_params AS (
SELECT 
	--'processed.f20171005fgcbdae84fb9ea1_nsi'::text as a,
    COALESCE((opt->>'a')::text,'') as a,
    COALESCE((opt->>'condition_a')::text,'TRUE') as condition_a,
    COALESCE((opt->>'b')::text,'') as b,
    COALESCE((opt->>'condition_b')::text,'TRUE') as condition_b,
	COALESCE((opt->>'tolerance')::text,'0') as tolerance,
    COALESCE((opt->>'temp')::boolean,FALSE) as _temp,
    COALESCE((opt->>'subfix')::text,'_overlap') as subfix,
    COALESCE((opt->>'schema')::text,'temp') as _schema,
    COALESCE((opt->>'add_geoinfo')::boolean,FALSE) as add_geoinfo,
    COALESCE((opt->>'add_diff')::boolean,FALSE) as add_diff,
    COALESCE((opt->>'min_sup')::real,0::real) as min_sup,
    COALESCE((opt->>'add_sup_total')::boolean,FALSE) as add_sup_total,
    COALESCE((opt->>'filter_overlap')::boolean,TRUE) as filter_overlap
 FROM (
 	SELECT 
    /* USAR PARA PRUEBAS
    json_build_object(
    	'a', 'processed.f20170718fagebdcf580ac83_nsi',
        --'a', 'processed.f20171027efcbagd4d9e6f3b_nsi',
        'b', 'coberturas.pdm',
        'add_sup_total', TRUE,
        'add_diff', TRUE,
        'filter_overlap', FALSE,
        'min_sup', 1
    ) */
    _opt 
    as opt
 ) params
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
            'a',a,
            'condition_a',condition_a
            'b',b,
            'condition_b',condition_b,
            'schema', _schema,
            'temp', _temp, --TRUE,
            'filter_overlap',filter_overlap,
            'add_sup_total',add_sup_total,
            'min_sup', min_sup,
            'add_geoinfo', add_geoinfo
        )
    ) as res_inter
    FROM _params limit 1
),
_difference AS (
    SELECT
    CASE WHEN add_diff THEN 
        sicob_difference(
            json_build_object(
                'a',(SELECT lyr_adjusted FROM _adjusted LIMIT 1),
                'b',(SELECT res_inter->>'lyr_intersected' FROM _intersected LIMIT 1),
                'schema', _schema,
                'temp',TRUE,
                'add_geoinfo', add_geoinfo,
                'filter_overlap',filter_overlap,
                'add_sup_total',add_sup_total,
				'min_sup', min_sup        
            )
        )
	ELSE
    	'{}'::json 
    END as res_diff
    FROM _params limit 1
),
_buildoverlap AS (
	SELECT
        CASE WHEN lyr_difference <> '' OR lyr_intersected <> '' THEN -->Si existen datos
            sicob_executesql(
                CASE WHEN lyr_intersected <> '' THEN
                    '(SELECT id_a, id_b' ||
                    CASE WHEN (opt->>'add_sup_total')::boolean
                        OR (opt->>'add_geoinfo')::boolean  
                        OR (opt->>'min_sup')::real > 0 THEN 
                        ', sicob_utm, sicob_sup' 
                    ELSE 
                        '' 
                    END ||
                    ', the_geom' || 
                    ' FROM ' || lyr_intersected || ')'
                ELSE
                    ''::text
                END ||
                CASE WHEN lyr_difference <> '' AND lyr_intersected <> '' THEN
                  ' UNION ALL '
                ELSE
                    ''::text
                END ||
                CASE WHEN lyr_difference <> '' THEN
                    '(SELECT id_a, null::integer as id_b' ||
                    CASE WHEN (opt->>'add_sup_total')::boolean
                        OR (opt->>'add_geoinfo')::boolean  
                        OR (opt->>'min_sup')::real > 0 THEN 
                        ', sicob_utm, sicob_sup' 
                    ELSE 
                        '' 
                    END ||
                    ', the_geom' || 
                    ' FROM ' || lyr_difference || ')'
                ELSE
                    ''::text
                END ,
                json_build_object(
                      'table_out',  (SELECT table_name || '___overlap' FROM sicob_split_table_name(opt->>'a') limit 1) ,
                      'temp', true
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
    	CASE WHEN res_overlap->>'table_out' <> '' THEN -->Si existen datos
            sicob_executesql(
                'SELECT row_number() over() as sicob_id, ''' || 
                (opt->>'a')::text || '''::text as source_a, ''' || 
                (opt->>'b')::text || '''::text as source_b,' || 
                columns_b || ', t.*
                FROM(
                    SELECT * FROM ' ||
                    (res_overlap->>'table_out'::text) || 
                    ' ORDER BY id_a, id_b 
                ) t LEFT JOIN ' || (opt->>'b') || ' b ON (b.sicob_id = t.id_b)
                ',
                json_build_object(
                  'table_out',  (SELECT opt->>'_schema'::text || '.' || table_name || (opt->>'subfix')::text FROM sicob_split_table_name(opt->>'a') limit 1) ,
                  'temp', opt->>'_temp',
                  'create_index', true
                )
            )
		ELSE
        	res_overlap
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
    	res_intersected::jsonb || res_difference::jsonb as summary 
 	FROM(
 		SELECT
        	(SELECT row_to_json(t) FROM _params t) as opt,
            (SELECT * FROM _intersected) as res_intersected,
            (SELECT * FROM _difference) as res_difference
	)r
)
SELECT
    jsonb_strip_nulls(
        json_build_object(
        'lyr_over' , res_overlap->>'table_out',
        'sicob_sup_total', COALESCE((summary->>'inters_sup')::real,0::real) + COALESCE((summary->>'diff_sup')::real, 0::REAL)
        )::jsonb || r.summary::jsonb
     )::json
 FROM(
 	SELECT
 		(SELECT res_overlap FROM _overlap) as res_overlap,
        (SELECT row_to_json(t) FROM _params t) as opt,
        (SELECT summary::json FROM _statistics) as summary
      
 ) r 
********/ 
/*
(SELECT * FROM _intersected) as res_inter,
(SELECT * FROM _difference) as res_diff,
(SELECT * FROM _adjusted) as res_adjust
*/
$function$
 