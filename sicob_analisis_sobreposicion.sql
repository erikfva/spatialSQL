SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_analisis_sobreposicion(_opt json)
 RETURNS json
 LANGUAGE plpgsql
AS $function$
DECLARE 
  _lyr_in TEXT;
  _workers integer;
  _aux json := '{}'; 
  _out json := '{}'; 
  b TEXT;
  _superficie_in float;
  sql text;
  detalle json;
 _doanalisys text[] DEFAULT ARRAY['ATE', 'ASL', 'PGMF', 'POAF', 'POP', 'PDM', 'RF', 'RPPN', 'TPFP', 'PLUS', 'D337', 'DPAS', 'APN', 'APD', 'APM'];
	listSQL TEXT[];
    analisys text;
    tbl_name text;
    _paralell_opt json := '{}';
	_partial_result json := '{}';
    _out_shp json := '[]'::json; --'{"lyr_list":[{"lyr": "processed.f20170704efgdcab41a98021_nsi", "fname": "Capa1", "condition":  {"nombre": "Poligono2"} }]}';

BEGIN
---------------------------
--PARAMETROS DE ENTRADA
---------------------------
--> lyr_in : capa para el analisis.
--> condition (opcional): Filtro para los datos de "lyr_in". Si no se especifica, se toman todos los registros.
--> doanalisys : Array del listado de analisis a aplicar, ej. "doanalisys":["TPFP","ASL"].
--> 	Si no se especifica se realizan todos los analisis.
--> workers : Cantidad de procesos para trabajar en paralelo, por defecto es 1 con lo se trabaja de forma secuencial (mas lento), se recomienda de 3 a 7 para no sobrecargar.
---------------------------
--VALORES DEVUELTOS
---------------------------
-- PARA CADA CAPA ANALIZADA :
--> lyr_over : capa resultante de la sobreposicion.
--> lyr_b : capa sobre la cual se sobrepone.
--> nombre : nombre de la capa sobrepuesta.
--> features_inters_cnt : cantidad de poligonos que se intersectan.
--> features_diff_cnt : cantidad de poligonos que NO se intersectan.
--> sicob_sup_total : Si existe intersecciÃ³n, devuelve la superficie total en ha. que se intersecta.
--> detalle: Array json, con las filas de los registros de la capa resultante.
--> porcentaje_sup: Valor de 0 a 100 que indica el porcentaje de superficie sobrepuesta.
--> sicob_sup_total: Superficie total en ha. de la capa de entrada, calculada por el sistema.

--_opt := '{"lyr_in":"uploads.f20170704gcfebdac5d7c097","doanalisys":["TPFP","ASL","ATE","PLUS"]}'::json;
--_opt := '{"lyr_in":"processed.f20171107adgecfb25a069f4_nsi"}'::json;
--_opt := '{"lyr_in":"processed.f20170809cfabdeg77d7b0d4_nsi","doanalisys":["ATE"],"workers":"5"}'::json;
IF COALESCE( (_opt->>'doanalisys')::text , '') <> '' THEN
	SELECT array_agg(regexp_replace(u::text,'"','','g')) FROM
	(
		SELECT json_array_elements(((_opt::json)->>'doanalisys')::json) as u
	) v INTO _doanalisys;
END IF;

_workers := COALESCE( (_opt->>'workers')::int, 1);

IF array_length(_doanalisys, 1)>2 AND _workers > 1 THEN
	/*
    i := 0;
	LOOP
    	IF i = array_length(_doanalisys, 1) THEN
        	EXIT;
        END IF;
        i := i + 1;
        SELECT array_append(partial_analisys, _doanalisys[i]::text) INTO partial_analisys;        
        IF i = array_length(_doanalisys, 1) OR array_length(partial_analisys, 1) = 2 THEN
            _paralell_opt := _opt::jsonb || 
                                jsonb_build_object(
                                    'doanalisys', partial_analisys::text[]
                                );
            sql := 'SELECT sicob_analisis_sobreposicion('|| QUOTE_LITERAL(_paralell_opt) || '::json)';
            SELECT array_append(listSQL, sql::text) INTO listSQL;  
            SELECT ARRAY[]::text[] INTO  partial_analisys;      
        END IF;
        
    END LOOP;
    */
    
    --GENERANDO UNA CONSULTA PARA CADA ANALISIS. 
	FOREACH analisys IN ARRAY _doanalisys LOOP
        _paralell_opt := _opt::jsonb || 
                            jsonb_build_object(
                                'doanalisys',ARRAY[analisys::text]
                            );
        sql := 'SELECT sicob_analisis_sobreposicion('|| QUOTE_LITERAL(_paralell_opt) || '::json)';
        SELECT array_append(listSQL, sql::text) INTO listSQL;
	END LOOP;   
    
    --EJECUTANDO LOS ANALISIS DE FORMA PARALELA.     	
    select sicob_paralellsql(listSQL,('{"max_connections":' || _workers || '}')::json) into _aux;
    
    --CONSOLIDANDO LOS RESULTADOS PARCIALES EN UNO SOLO
    FOR _partial_result IN SELECT * FROM json_array_elements(_aux) LOOP
    	_out := _out::jsonb || (_partial_result->'result')::jsonb;
         _out_shp := _out_shp::jsonb || ((_partial_result->'result')->'lyr_list')::jsonb;
    END LOOP;
    
    RETURN _out::jsonb || jsonb_build_object('lyr_list',_out_shp);
END IF;


_lyr_in := _opt->>'lyr_in';
SELECT (sicob_split_table_name(_lyr_in)).table_name INTO tbl_name ;
    
sql := 'SELECT array_to_json(array_agg(q)) as detalle
FROM (
	SELECT *, round(cast(sicob_maxlimit(sicob_sup_sob * 100 / sicob_sup ,100) as numeric),1) as porcentaje
	FROM(
		SELECT 
          s.sicob_id, 
          s.sicob_sup,
          (SELECT sum (t.sicob_sup) FROM %s t
             WHERE t.id_a = s.sicob_id 
			) as sicob_sup_sob
        FROM
        	%s s
	) r
WHERE r.sicob_sup_sob IS NOT NULL
) q';

EXECUTE 'SELECT sum(sicob_sup) FROM ' || _lyr_in ||
' WHERE TRUE ' || COALESCE( 'AND (' ||  (_opt->>'condition')::text || ')', '') INTO _superficie_in;

----------------------------------------------
--Autorizaciones Transitorias Especiales (ATE)
----------------------------------------------
IF 'ATE' = ANY(_doanalisys) THEN
    _aux := sicob_overlap(
    	json_build_object(
        	'a', _lyr_in,
            'condition_a', COALESCE( (_opt->>'condition')::text , 'TRUE'),
            'b', 'coberturas.ate',
            'condition_b', 'est=''VIGENTE''',
            'subfix', '_ate',
            'schema', 'temp',
            'add_sup_total', true,
            'filter_overlap', false
        )
    );
    IF COALESCE( (_aux->>'features_inters_cnt')::int,0) > 0 THEN --> Si se ha encontrado sobreposicion.
        -->Almacenando la informacion para generar shapefiles    	
        _out_shp := _out_shp::jsonb || 
        jsonb_build_object(
            'lyr',_aux->>'lyr_over',
            'fname', 'sobreposicion_ate'
        ) || 
        jsonb_build_object(
            'lyr', sicob_executesql('
                        SELECT DISTINCT a.* 
                        FROM coberturas.ate a INNER JOIN ' || (_aux->>'lyr_over')::text || ' b 
                        ON (a.sicob_id = b.id_b)
                    ',
                        ('{"table_out":"temp.ate_' || tbl_name || '"}')::json
                    )->>'table_out',
            'fname', 'ate'
        );        
        
        _aux := _aux::jsonb || ('{"lyr_b":"coberturas.ate","nombre":"Autorizaciones Transitorias Especiales (ATE)","porcentaje_sup":"' || (round(((_aux->>'sicob_sup_total')::float *100/_superficie_in)::numeric,1))::text || '"}')::jsonb;
        
            EXECUTE format('SELECT array_to_json(array_agg(q)) as detalle
            FROM (
                SELECT ' || sicob_no_geo_column(_aux->>'lyr_over','{}','b.') || ',
                    round(cast((b.sicob_sup * 100 / a.sicob_sup) as numeric),1) as PCTJE
                FROM %s a, %s b
                WHERE a.sicob_id = b.id_a
            ) q', _lyr_in, _aux->>'lyr_over') INTO detalle;    
        
        _aux := _aux::jsonb || ('{"detalle":' || detalle || '}')::jsonb;
        _out := ('{"ATE":' || _aux::text || '}')::json;
                
    END IF;
END IF;
----------------------------------------------
--AsociaciÃ³n Sociales del Lugar (ASL)
----------------------------------------------
IF 'ASL' = ANY (_doanalisys) THEN
    _aux := sicob_overlap(
    	json_build_object(
        	'a', _lyr_in,
            'condition_a', COALESCE( (_opt->>'condition')::text , 'TRUE'),
            'b', 'coberturas.asl',
            'subfix', '_asl',
            'schema', 'temp',
            'add_sup_total', true,
            'filter_overlap', false
        )    
	);
    IF COALESCE( (_aux->>'features_inters_cnt')::int,0) > 0 THEN --> Si se ha encontrado sobreposicion.
        -->Almacenando la informacion para generar shapefiles    	
        _out_shp := _out_shp::jsonb || 
        jsonb_build_object(
            'lyr',_aux->>'lyr_over',
            'fname', 'sobreposicion_asl'
        ) || 
        jsonb_build_object(
            'lyr', sicob_executesql('
                        SELECT DISTINCT a.* 
                        FROM coberturas.asl a INNER JOIN ' || (_aux->>'lyr_over')::text || ' b 
                        ON (a.sicob_id = b.id_b)
                    ',
                        ('{"table_out":"temp.asl_' || tbl_name || '"}')::json
                    )->>'table_out',
            'fname', 'asl'
        );      
    
        _aux := _aux::jsonb || ('{"lyr_b":"coberturas.asl","nombre":"AsociaciÃ³n Sociales del Lugar (ASL)","porcentaje_sup":"' || (round(((_aux->>'sicob_sup_total')::float *100/_superficie_in)::numeric,1))::text || '"}')::jsonb;
        
            EXECUTE format('SELECT array_to_json(array_agg(q)) as detalle
            FROM (
                SELECT ' || sicob_no_geo_column(_aux->>'lyr_over','{}','b.') || ',
                    round(cast((b.sicob_sup * 100 / a.sicob_sup) as numeric),1) as PCTJE
                FROM %s a, %s b
                WHERE a.sicob_id = b.id_a
            ) q', _lyr_in, _aux->>'lyr_over') INTO detalle;
            
        _aux := _aux::jsonb || ('{"detalle":' || detalle || '}')::jsonb;
        _out := _out::jsonb || ('{"ASL":' || _aux::text || '}')::jsonb; 
    END IF;
END IF;
----------------------------------------------
--Plan Gral. de Manejo Forestal  (PGMF)
----------------------------------------------
IF 'PGMF' = ANY (_doanalisys) THEN
    _aux := sicob_overlap(
    	json_build_object(
        	'a', _lyr_in,
            'condition_a', COALESCE( (_opt->>'condition')::text , 'TRUE'),
            'b', 'coberturas.pgmf',
            'subfix', '_pgmf',
            'schema', 'temp',
            'add_sup_total', true,
            'filter_overlap', false
        )    
	);
    IF COALESCE( (_aux->>'features_inters_cnt')::int,0) > 0 THEN --> Si se ha encontrado sobreposicion.
        
        -->Almacenando la informacion para generar shapefiles    	
        _out_shp := _out_shp::jsonb || 
        jsonb_build_object(
            'lyr',_aux->>'lyr_over',
            'fname', 'sobreposicion_pgmf'
        ) || 
        jsonb_build_object(
            'lyr', sicob_executesql('
                        SELECT DISTINCT a.* 
                        FROM coberturas.pgmf a INNER JOIN ' || (_aux->>'lyr_over')::text || ' b 
                        ON (a.sicob_id = b.id_b)
                    ',
                        ('{"table_out":"temp.pgmf_' || tbl_name || '"}')::json
                    )->>'table_out',
            'fname', 'pgmf'
        );
             
        _aux := _aux::jsonb || ('{"lyr_b":"coberturas.pgmf","nombre":"Plan Gral. de Manejo Forestal  (PGMF)","porcentaje_sup":"' || (round(((_aux->>'sicob_sup_total')::float *100/_superficie_in)::numeric,1))::text || '"}')::jsonb;
            EXECUTE format('SELECT array_to_json(array_agg(q)) as detalle
            FROM (
                SELECT ' || sicob_no_geo_column(_aux->>'lyr_over','{}','b.') || ',
                    round(cast((b.sicob_sup * 100 / a.sicob_sup) as numeric),1) as PCTJE
                FROM %s a, %s b
                WHERE a.sicob_id = b.id_a
            ) q', _lyr_in, _aux->>'lyr_over') INTO detalle;
        _aux := _aux::jsonb || ('{"detalle":' || detalle || '}')::jsonb;
        _out := _out::jsonb || ('{"PGMF":' || _aux::text || '}')::jsonb;
    END IF; 
END IF; 
----------------------------------------------
--Plan Operativo Anual Forestal (POAF)
----------------------------------------------
IF 'POAF' = ANY (_doanalisys) THEN
    _aux := sicob_overlap(
    	json_build_object(
        	'a', _lyr_in,
            'condition_a', COALESCE( (_opt->>'condition')::text , 'TRUE'),
            'b', 'coberturas.poaf',
            'subfix', '_poaf',
            'schema', 'temp',
            'add_sup_total', true,
            'filter_overlap', false
        )     
	);
    IF COALESCE( (_aux->>'features_inters_cnt')::int,0) > 0 THEN --> Si se ha encontrado sobreposicion.
        
        -->Almacenando la informacion para generar shapefiles    	
        _out_shp := _out_shp::jsonb || 
        jsonb_build_object(
            'lyr',_aux->>'lyr_over',
            'fname', 'sobreposicion_poaf'
        ) || 
        jsonb_build_object(
            'lyr', sicob_executesql('
                        SELECT DISTINCT a.* 
                        FROM coberturas.poaf a INNER JOIN ' || (_aux->>'lyr_over')::text || ' b 
                        ON (a.sicob_id = b.id_b)
                    ',
                        ('{"table_out":"temp.poaf_' || tbl_name || '"}')::json
                    )->>'table_out',
            'fname', 'poaf'
        );
            
        _aux := _aux::jsonb || ('{"lyr_b":"coberturas.poaf","nombre":"Plan Operativo Anual Forestal (POAF)","porcentaje_sup":"' || (round(((_aux->>'sicob_sup_total')::float *100/_superficie_in)::numeric,1))::text || '"}')::jsonb;
            EXECUTE format('SELECT array_to_json(array_agg(q)) as detalle
            FROM (
                SELECT ' || sicob_no_geo_column(_aux->>'lyr_over','{}','b.') || ',
                    round(cast((b.sicob_sup * 100 / a.sicob_sup) as numeric),1) as PCTJE
                FROM %s a, %s b
                WHERE a.sicob_id = b.id_a
            ) q', _lyr_in, _aux->>'lyr_over') INTO detalle;
        _aux := _aux::jsonb || ('{"detalle":' || detalle || '}')::jsonb;
        _out := _out::jsonb || ('{"POAF":' || _aux::text || '}')::jsonb; 
    END IF;
END IF;
----------------------------------------------
--Plan de Ordenamiento  Predial (POP)
----------------------------------------------
IF 'POP' = ANY (_doanalisys) THEN
    _aux := sicob_overlap(
    	json_build_object(
        	'a', _lyr_in,
            'condition_a', COALESCE( (_opt->>'condition')::text , 'TRUE'),
            'b', 'coberturas.pop_uso_vigente',
            'subfix', '_pop_uso',
            'schema', 'temp',
            'add_sup_total', true,
            'filter_overlap', false
        )    
    );
    IF COALESCE( (_aux->>'features_inters_cnt')::int,0) > 0 THEN --> Si se ha encontrado sobreposicion.
        
        -->Almacenando la informacion para generar shapefiles    	
        _out_shp := _out_shp::jsonb || 
        jsonb_build_object(
            'lyr',_aux->>'lyr_over',
            'fname', 'sobreposicion_pop_uso'
        );
        _partial_result := sicob_executesql('
                        SELECT DISTINCT a.* 
                        FROM coberturas.pop a INNER JOIN ' || (_aux->>'lyr_over')::text || ' b 
                        ON (a.sicob_id = b.id_b)
                    ',
                        ('{"table_out":"temp.pop_' || tbl_name || '"}')::json
                    );
		IF (_partial_result->>'row_cnt')::int > 0 THEN
        	_out_shp := _out_shp::jsonb || 
            			jsonb_build_object(
                            'lyr', _partial_result->>'table_out',
                            'fname', 'pop'
                        );
        END IF;         
        

        
        _aux := _aux::jsonb || ('{"lyr_b":"coberturas.pop_uso_vigente","nombre":"Plan de Ordenamiento  Predial (POP)","porcentaje_sup":"' || (round(((_aux->>'sicob_sup_total')::float *100/_superficie_in)::numeric,1))::text || '"}')::jsonb;
         EXECUTE '
         SELECT array_to_json(array_agg(q)) as detalle
        FROM(
         SELECT id_a as sicob_id, res_adm as resol_pop, uso_pop, des_uso, sum(sicob_sup) as sicob_sup FROM ' || (_aux->>'lyr_over')::text || ' group by id_a, res_adm, uso_pop,des_uso)q;' INTO detalle;
        _aux := _aux::jsonb || ('{"detalle":' || detalle || '}')::jsonb;
        _out := _out::jsonb || ('{"POP_USO":' || _aux::text || '}')::jsonb; 
    END IF;
END IF;
----------------------------------------------
--Plan de Desmonte (PDM)
----------------------------------------------
IF 'PDM' = ANY (_doanalisys) THEN
    _aux := sicob_overlap(
    	json_build_object(
        	'a', _lyr_in,
            'condition_a', COALESCE( (_opt->>'condition')::text , 'TRUE'),
            'b', 'coberturas.pdm',
            'subfix', '_pdm',
            'schema', 'temp',
            'add_sup_total', true,
            'filter_overlap', false
        )    
	);
    IF COALESCE( (_aux->>'features_inters_cnt')::int,0) > 0 THEN --> Si se ha encontrado sobreposicion.
        
        -->Almacenando la informacion para generar shapefiles    	
        _out_shp := _out_shp::jsonb || 
        jsonb_build_object(
            'lyr',_aux->>'lyr_over',
            'fname', 'sobreposicion_pdm'
        ) || 
        jsonb_build_object(
            'lyr', sicob_executesql('
                        SELECT DISTINCT a.* 
                        FROM coberturas.pdm a INNER JOIN ' || (_aux->>'lyr_over')::text || ' b 
                        ON (a.sicob_id = b.id_b)
                    ',
                        ('{"table_out":"temp.pdm_' || tbl_name || '"}')::json
                    )->>'table_out',
            'fname', 'pdm'
        );
                
        _aux := _aux::jsonb || ('{"lyr_b":"coberturas.pdm","nombre":"Plan de Desmonte (PDM)","porcentaje_sup":"' || (round(((_aux->>'sicob_sup_total')::float *100/_superficie_in)::numeric,1))::text || '"}')::jsonb;
        EXECUTE '
        SELECT array_to_json(array_agg(q)) as detalle
        FROM(
            select id_a as sicob_id, res_adm, to_char(fec_res, ''YYYY-MM-DD'') as fec_res, nom_pre, sum(sicob_sup) as sicob_sup_sob,
            count(*) as cantidad_poligonos 
            from ' || (_aux->>'lyr_over')::text || ' group by id_a, res_adm,res_adm, fec_res, nom_pre
            order by fec_res
         )q;' INTO detalle;
         --EXECUTE format(sql,_aux->>'lyr_over',_lyr_in) INTO detalle;
        _aux := _aux::jsonb || ('{"detalle":' || detalle || '}')::jsonb;
        _out := _out::jsonb || ('{"PDM":' || _aux::text || '}')::jsonb; 
    END IF;
END IF;
----------------------------------------------
--Reservas Forestales (RF)
----------------------------------------------
IF 'RF' = ANY (_doanalisys) THEN
    _aux := sicob_overlap(
    	json_build_object(
        	'a', _lyr_in,
            'condition_a', COALESCE( (_opt->>'condition')::text , 'TRUE'),
            'b', 'coberturas.rf',
            'subfix', '_rf',
            'schema', 'temp',
            'add_sup_total', true,
            'filter_overlap', false
        )    
	);
    IF COALESCE( (_aux->>'features_inters_cnt')::int,0) > 0 THEN --> Si se ha encontrado sobreposicion.
        
        -->Almacenando la informacion para generar shapefiles    	
        _out_shp := _out_shp::jsonb || 
        jsonb_build_object(
            'lyr',_aux->>'lyr_over',
            'fname', 'sobreposicion_rf'
        ) || 
        jsonb_build_object(
            'lyr', sicob_executesql('
                        SELECT DISTINCT a.* 
                        FROM coberturas.rf a INNER JOIN ' || (_aux->>'lyr_over')::text || ' b 
                        ON (a.sicob_id = b.id_b)
                    ',
                        ('{"table_out":"temp.rf_' || tbl_name || '"}')::json
                    )->>'table_out',
            'fname', 'rf'
        );
        
        _aux := _aux::jsonb || ('{"lyr_b":"coberturas.rf","nombre":"Reservas Forestales (RF)","porcentaje_sup":"' || (round(((_aux->>'sicob_sup_total')::float *100/_superficie_in)::numeric,1))::text || '"}')::jsonb;
        
            EXECUTE format('SELECT array_to_json(array_agg(q)) as detalle
            FROM (
                SELECT ' || sicob_no_geo_column(_aux->>'lyr_over','{}','b.') || ',
                    round(cast((b.sicob_sup * 100 / a.sicob_sup) as numeric),1) as PCTJE
                FROM %s a, %s b
                WHERE a.sicob_id = b.id_a
            ) q', _lyr_in, _aux->>'lyr_over') INTO detalle;
            
        _aux := _aux::jsonb || ('{"detalle":' || detalle || '}')::jsonb;
        _out := _out::jsonb || ('{"RF":' || _aux::text || '}')::jsonb;
    END IF; 
END IF;
----------------------------------------------
--Reservas Privada de Patrimonio  Natural (RPPN)
----------------------------------------------
IF 'RPPN' = ANY (_doanalisys) THEN
    _aux := sicob_overlap(
    	json_build_object(
        	'a', _lyr_in,
            'condition_a', COALESCE( (_opt->>'condition')::text , 'TRUE'),
            'b', 'coberturas.rppn',
            'subfix', '_rppn',
            'schema', 'temp',
            'add_sup_total', true,
            'filter_overlap', false
        )    
	);
    IF COALESCE( (_aux->>'features_inters_cnt')::int,0) > 0 THEN --> Si se ha encontrado sobreposicion.
        
        -->Almacenando la informacion para generar shapefiles    	
        _out_shp := _out_shp::jsonb || 
        jsonb_build_object(
            'lyr',_aux->>'lyr_over',
            'fname', 'sobreposicion_rppn'
        ) || 
        jsonb_build_object(
            'lyr', sicob_executesql('
                        SELECT DISTINCT a.* 
                        FROM coberturas.rppn a INNER JOIN ' || (_aux->>'lyr_over')::text || ' b 
                        ON (a.sicob_id = b.id_b)
                    ',
                        ('{"table_out":"temp.rppn_' || tbl_name || '"}')::json
                    )->>'table_out',
            'fname', 'rppn'
        );
                
        _aux := _aux::jsonb || ('{"lyr_b":"coberturas.rppn","nombre":"Reservas Privada de Patrimonio Natural (RPPN)","porcentaje_sup":"' || (round(((_aux->>'sicob_sup_total')::float *100/_superficie_in)::numeric,1))::text || '"}')::jsonb;
            EXECUTE format('SELECT array_to_json(array_agg(q)) as detalle
            FROM (
                SELECT ' || sicob_no_geo_column(_aux->>'lyr_over','{}','b.') || ',
                    round(cast((b.sicob_sup * 100 / a.sicob_sup) as numeric),1) as PCTJE
                FROM %s a, %s b
                WHERE a.sicob_id = b.id_a
            ) q', _lyr_in, _aux->>'lyr_over') INTO detalle;
        _aux := _aux::jsonb || ('{"detalle":' || detalle || '}')::jsonb;
        _out := _out::jsonb || ('{"RPPN":' || _aux::text || '}')::jsonb; 
    END IF;
END IF;
----------------------------------------------
--Tierras de Produccion Forestal Permanente (TPFP)
----------------------------------------------
IF 'TPFP' = ANY (_doanalisys) THEN
    _aux := sicob_overlap(
    	json_build_object(
        	'a', _lyr_in,
            'condition_a', COALESCE( (_opt->>'condition')::text , 'TRUE'),
            'b', 'coberturas.tpfp',
            'subfix', '_tpfp',
            'schema', 'temp',
            'add_sup_total', true
        )    
    );
    IF COALESCE( (_aux->>'features_inters_cnt')::int,0) > 0 THEN --> Si se ha encontrado sobreposicion.
        
        -->Almacenando la informacion para generar shapefiles    	
        _out_shp := _out_shp::jsonb || 
        jsonb_build_object(
            'lyr',_aux->>'lyr_over',
            'fname', 'tpfp'
        );
        
        _aux := _aux::jsonb || ('{"lyr_b":"coberturas.tpfp","nombre":"Tierras de Produccion Forestal Permanente (TPFP)","porcentaje_sup":"' || (round(((_aux->>'sicob_sup_total')::float *100/_superficie_in)::numeric,1))::text || '"}')::jsonb;
        
            EXECUTE format('SELECT array_to_json(array_agg(q)) as detalle
            FROM (
                SELECT ' || sicob_no_geo_column(_aux->>'lyr_over','{}','b.') || ',
                    round(cast((b.sicob_sup * 100 / a.sicob_sup) as numeric),1) as PCTJE
                FROM %s a, %s b
                WHERE a.sicob_id = b.id_a
            ) q', _lyr_in, _aux->>'lyr_over') INTO detalle;
            
        _aux := _aux::jsonb || ('{"detalle":' || detalle || '}')::jsonb;
        
        _out := _out::jsonb || ('{"TPFP":' || _aux::text || '}')::jsonb; 
    END IF;
END IF;
----------------------------------------------
--Plan de Uso de Suelo (PLUS)
----------------------------------------------
IF 'PLUS' = ANY (_doanalisys) THEN
    _aux := sicob_overlap(('{"a":"' || _lyr_in || '","condition_a":"' || COALESCE( (_opt->>'condition')::text , 'TRUE') || '", "b":"coberturas.plus","subfix":"_plus", "add_diff":true,"schema":"temp","add_sup_total":true, "add_geoinfo":true}')::json);
    IF COALESCE( (_aux->>'features_inters_cnt')::int,0) > 0 THEN --> Si se ha encontrado sobreposicion.
		
        -->Almacenando la informacion para generar shapefiles    	
        _out_shp := _out_shp::jsonb || 
        	jsonb_build_object(
            	'lyr',_aux->>'lyr_over',
                'fname', 'plus',
                'condition',  '{"id_b": "!null"}'::json
            ); 
        
        _aux := _aux::jsonb || ('{"lyr_b":"coberturas.plus","nombre":"Plan de Uso de Suelo (PLUS)","porcentaje_sup":"' || (round(((_aux->>'sicob_sup_total')::float *100/_superficie_in)::numeric,1))::text || '"}')::jsonb;
         EXECUTE format('SELECT array_to_json(array_agg(q)) as detalle
    FROM (
      SELECT 
        b.id_a,
        COALESCE(b.categoria,''SIN INFORMACION'') as categoria,
        COALESCE(b.subcategoria,''SIN INFORMACION'') as subcategoria,
        COALESCE(b.codigo,''SIN INFORMACION'') as codigo,
        b.sicob_sup,
        round(cast((b.sicob_sup * 100 / a.sicob_sup) as numeric),1) as PCTJE,
        b.sicob_id,
        b.source
      FROM
        %s a
        INNER JOIN %s b ON (a.sicob_id = b.id_a)
      ORDER BY
        id_a, categoria
    ) q', _lyr_in, _aux->>'lyr_over') INTO detalle;
        _aux := _aux::jsonb || ('{"detalle":' || detalle || '}')::jsonb;
        _out := _out::jsonb || ('{"PLUS":' || _aux::text || '}')::jsonb; 
    END IF;

    IF COALESCE( (_aux->>'features_diff_cnt')::int,0) > 0 THEN --> Si existen poligonos sin encontrar sobreposicion en PLUS, se busca en CUMAT.
    ----------------------------------------------
    --Capacidad de Uso Mayor de la Tierra (CUMAT)
    ----------------------------------------------
        b := _aux->>'lyr_over';
        _aux := sicob_overlap(('{"a":"' || b || '","condition_a":"codigo IS NULL", "b":"coberturas.cumat","subfix":"_cumat", "add_diff":true,"schema":"temp","add_sup_total":true, "add_geoinfo":true}')::json);
        IF COALESCE( (_aux->>'features_inters_cnt')::int,0) > 0 THEN --> Si se ha encontrado sobreposicion.
        	
            -->Almacenando la informacion para generar shapefiles    	
        	_out_shp := _out_shp::jsonb || 
        	jsonb_build_object(
            	'lyr',_aux->>'lyr_over',
                'fname', 'cumat'
            ); 
            
            -->Cambiando la referencia de "a" hacia la tabla de entrada "_lyr_in"
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
            ',_aux->>'lyr_over',b, _lyr_in);

            EXECUTE format('SELECT array_to_json(array_agg(q)) as detalle
            FROM (
                SELECT ' || sicob_no_geo_column(_aux->>'lyr_over','{}','b.') || ',
                    round(cast((b.sicob_sup * 100 / a.sicob_sup) as numeric),1) as PCTJE
                FROM %s a, %s b
                WHERE a.sicob_id = b.id_a
            ) q', _lyr_in, _aux->>'lyr_over') INTO detalle;
            _aux := _aux::jsonb || ('{"detalle":' || detalle || '}')::jsonb;
            
            _aux := _aux::jsonb || ('{"lyr_b":"coberturas.cumat","nombre":"Capacidad de Uso Mayor de la Tierra (CUMAT)","porcentaje_sup":"' || (round(((_aux->>'sicob_sup_total')::float *100/_superficie_in)::numeric,1))::text || '"}')::jsonb;
            _out := _out::jsonb || ('{"CUMAT":' || _aux::text || '}')::jsonb; 
        END IF;
    END IF;
END IF;

----------------------------------------------
--Desmontes Inscritos Ley 337 (D337)
----------------------------------------------
IF 'D337' = ANY (_doanalisys) THEN
    _aux := sicob_overlap(('{"a":"' || _lyr_in || '","condition_a":"' || COALESCE( (_opt->>'condition')::text , 'TRUE') || '", "b":"coberturas.d337","subfix":"_d337","schema":"temp","add_sup_total":true}')::json);
    IF COALESCE( (_aux->>'features_inters_cnt')::int,0) > 0 THEN --> Si se han encontrado desmontes.
    
        -->Almacenando la informacion para generar shapefiles    	
        _out_shp := _out_shp::jsonb || 
        jsonb_build_object(
            'lyr',_aux->>'lyr_over',
            'fname', 'sobreposicion_d337'
        ) || 
        jsonb_build_object(
            'lyr', sicob_executesql('
                        SELECT DISTINCT a.* 
                        FROM coberturas.d337 a INNER JOIN ' || (_aux->>'lyr_over')::text || ' b 
                        ON (a.sicob_id = b.id_b)
                    ',
                        ('{"table_out":"temp.d337_' || tbl_name || '"}')::json
                    )->>'table_out',
            'fname', 'd337'
        );
            
        _aux := _aux::jsonb || ('{"lyr_b":"coberturas.d337","nombre":"Desmontes inscritos Programa de Produccion de Alimentos","porcentaje_sup":"' || (round(((_aux->>'sicob_sup_total')::float *100/_superficie_in)::numeric,1))::text || '"}')::jsonb;
            EXECUTE format('SELECT array_to_json(array_agg(q)) as detalle
            FROM (
                SELECT ' || sicob_no_geo_column(_aux->>'lyr_over','{}','b.') || ',
                    round(cast((b.sicob_sup * 100 / a.sicob_sup) as numeric),1) as PCTJE
                FROM %s a, %s b
                WHERE a.sicob_id = b.id_a
            ) q', _lyr_in, _aux->>'lyr_over') INTO detalle;
        _aux := _aux::jsonb || ('{"detalle":' || detalle || '}')::jsonb;
        _out := _out::jsonb || ('{"D337":' || _aux::text || '}')::jsonb; 
    END IF;
END IF;

----------------------------------------------
--Desmontes Ilegales con Proceso Administrativo Sancionatorio (DPAS)
----------------------------------------------
IF 'DPAS' = ANY (_doanalisys) THEN
    _aux := sicob_overlap(('{"a":"' || _lyr_in || '","condition_a":"' || COALESCE( (_opt->>'condition')::text , 'TRUE') || '", "b":"coberturas.dpas","subfix":"_dpas","schema":"temp","add_sup_total":true}')::json);
    IF COALESCE( (_aux->>'features_inters_cnt')::int,0) > 0 THEN --> Si se han encontrado desmontes.
        
        -->Almacenando la informacion para generar shapefiles    	
        _out_shp := _out_shp::jsonb || 
        jsonb_build_object(
            'lyr',_aux->>'lyr_over',
            'fname', 'sobreposicion_dpas'
        ) || 
        jsonb_build_object(
            'lyr', sicob_executesql('
                        SELECT DISTINCT a.* 
                        FROM coberturas.dpas a INNER JOIN ' || (_aux->>'lyr_over')::text || ' b 
                        ON (a.sicob_id = b.id_b)
                    ',
                        ('{"table_out":"temp.dpas_' || tbl_name || '"}')::json
                    )->>'table_out',
            'fname', 'dpas'
        );
        
        _aux := _aux::jsonb || ('{"lyr_b":"coberturas.dpas","nombre":"Desmontes ilegales con Proceso Administrativo Sancionatorio (DPAS)","porcentaje_sup":"' || (round(((_aux->>'sicob_sup_total')::float *100/_superficie_in)::numeric,1))::text || '"}')::jsonb;
            EXECUTE format('SELECT array_to_json(array_agg(q)) as detalle
            FROM (
                SELECT ' || sicob_no_geo_column(_aux->>'lyr_over','{}','b.') || ',
                    round(cast((b.sicob_sup * 100 / a.sicob_sup) as numeric),1) as PCTJE
                FROM %s a, %s b
                WHERE a.sicob_id = b.id_a
            ) q', _lyr_in, _aux->>'lyr_over') INTO detalle;
        _aux := _aux::jsonb || ('{"detalle":' || detalle || '}')::jsonb;
        _out := _out::jsonb || ('{"DPAS":' || _aux::text || '}')::jsonb; 
    END IF;
END IF;

----------------------------------------------
--Areas Protegidas Nacionales (APN)
----------------------------------------------
IF 'APN' = ANY (_doanalisys) THEN
    _aux := sicob_overlap(('{"a":"' || _lyr_in || '","condition_a":"' || COALESCE( (_opt->>'condition')::text , 'TRUE') || '", "b":"coberturas.apn","subfix":"_apn","schema":"temp","add_sup_total":true,"filter_overlap":false}')::json);
    IF COALESCE( (_aux->>'features_inters_cnt')::int,0) > 0 THEN --> Si se ha encontrado sobreposicion.
        
        -->Almacenando la informacion para generar shapefiles    	
        _out_shp := _out_shp::jsonb || 
        jsonb_build_object(
            'lyr',_aux->>'lyr_over',
            'fname', 'sobreposicion_apn'
        ) || 
        jsonb_build_object(
            'lyr', sicob_executesql('
                        SELECT DISTINCT a.* 
                        FROM coberturas.apn a INNER JOIN ' || (_aux->>'lyr_over')::text || ' b 
                        ON (a.sicob_id = b.id_b)
                    ',
                        ('{"table_out":"temp.apn_' || tbl_name || '"}')::json
                    )->>'table_out',
            'fname', 'apn'
        );
            
        _aux := _aux::jsonb || ('{"lyr_b":"coberturas.apn","nombre":"Areas Protegidas Nacionales (APN)","porcentaje_sup":"' || (round(((_aux->>'sicob_sup_total')::float *100/_superficie_in)::numeric,1))::text || '"}')::jsonb;
            EXECUTE format('SELECT array_to_json(array_agg(q)) as detalle
            FROM (
                SELECT ' || sicob_no_geo_column(_aux->>'lyr_over','{}','b.') || ',
                    round(cast((b.sicob_sup * 100 / a.sicob_sup) as numeric),1) as PCTJE
                FROM %s a, %s b
                WHERE a.sicob_id = b.id_a
            ) q', _lyr_in, _aux->>'lyr_over') INTO detalle;
        _aux := _aux::jsonb || ('{"detalle":' || detalle || '}')::jsonb;
        _out := _out::jsonb || ('{"APN":' || _aux::text || '}')::jsonb; 
    END IF;
END IF;

----------------------------------------------
--Areas Protegidas Departamentales (APD)
----------------------------------------------
IF 'APD' = ANY (_doanalisys) THEN
    _aux := sicob_overlap(('{"a":"' || _lyr_in || '","condition_a":"' || COALESCE( (_opt->>'condition')::text , 'TRUE') || '", "b":"coberturas.apd","subfix":"_apd","schema":"temp","add_sup_total":true,"filter_overlap":false}')::json);
    IF COALESCE( (_aux->>'features_inters_cnt')::int,0) > 0 THEN --> Si se ha encontrado sobreposicion.
        
        -->Almacenando la informacion para generar shapefiles    	
        _out_shp := _out_shp::jsonb || 
        jsonb_build_object(
            'lyr',_aux->>'lyr_over',
            'fname', 'sobreposicion_apd'
        ) || 
        jsonb_build_object(
            'lyr', sicob_executesql('
                        SELECT DISTINCT a.* 
                        FROM coberturas.apd a INNER JOIN ' || (_aux->>'lyr_over')::text || ' b 
                        ON (a.sicob_id = b.id_b)
                    ',
                        ('{"table_out":"temp.apd_' || tbl_name || '"}')::json
                    )->>'table_out',
            'fname', 'apd'
        );
            
        _aux := _aux::jsonb || ('{"lyr_b":"coberturas.apd","nombre":"Areas Protegidas Departamentales (APD)","porcentaje_sup":"' || (round(((_aux->>'sicob_sup_total')::float *100/_superficie_in)::numeric,1))::text || '"}')::jsonb;
            EXECUTE format('SELECT array_to_json(array_agg(q)) as detalle
            FROM (
                SELECT ' || sicob_no_geo_column(_aux->>'lyr_over','{}','b.') || ',
                    round(cast((b.sicob_sup * 100 / a.sicob_sup) as numeric),1) as PCTJE
                FROM %s a, %s b
                WHERE a.sicob_id = b.id_a
            ) q', _lyr_in, _aux->>'lyr_over') INTO detalle;
        _aux := _aux::jsonb || ('{"detalle":' || detalle || '}')::jsonb;
        _out := _out::jsonb || ('{"APD":' || _aux::text || '}')::jsonb; 
    END IF;
END IF;

----------------------------------------------
--Areas Protegidas Municipales (APM)
----------------------------------------------
IF 'APM' = ANY (_doanalisys) THEN
    _aux := sicob_overlap(('{"a":"' || _lyr_in || '","condition_a":"' || COALESCE( (_opt->>'condition')::text , 'TRUE') || '", "b":"coberturas.apm","subfix":"_apm","schema":"temp","add_sup_total":true,"filter_overlap":false}')::json);
    IF COALESCE( (_aux->>'features_inters_cnt')::int,0) > 0 THEN --> Si se ha encontrado sobreposicion.
        
        -->Almacenando la informacion para generar shapefiles    	
        _out_shp := _out_shp::jsonb || 
        jsonb_build_object(
            'lyr',_aux->>'lyr_over',
            'fname', 'sobreposicion_apm'
        ) || 
        jsonb_build_object(
            'lyr', sicob_executesql('
                        SELECT DISTINCT a.* 
                        FROM coberturas.apm a INNER JOIN ' || (_aux->>'lyr_over')::text || ' b 
                        ON (a.sicob_id = b.id_b)
                    ',
                        ('{"table_out":"temp.apm_' || tbl_name || '"}')::json
                    )->>'table_out',
            'fname', 'apm'
        );
        
        _aux := _aux::jsonb || ('{"lyr_b":"coberturas.apm","nombre":"Areas Protegidas Municipales (APM)","porcentaje_sup":"' || (round(((_aux->>'sicob_sup_total')::float *100/_superficie_in)::numeric,1))::text || '"}')::jsonb;
            EXECUTE format('SELECT array_to_json(array_agg(q)) as detalle
            FROM (
                SELECT ' || sicob_no_geo_column(_aux->>'lyr_over','{}','b.') || ',
                    round(cast((b.sicob_sup * 100 / a.sicob_sup) as numeric),1) as PCTJE
                FROM %s a, %s b
                WHERE a.sicob_id = b.id_a
            ) q', _lyr_in, _aux->>'lyr_over') INTO detalle;
        _aux := _aux::jsonb || ('{"detalle":' || detalle || '}')::jsonb;
        _out := _out::jsonb || ('{"APM":' || _aux::text || '}')::jsonb; 
    END IF;
END IF;

RETURN _out::jsonb || jsonb_build_object('lyr_list',_out_shp);

EXCEPTION
WHEN others THEN
            RAISE EXCEPTION 'geoSICOB (sicob_analisis_sobreposicion): >> % (%)',SQLERRM, SQLSTATE;	
END;
$function$
 