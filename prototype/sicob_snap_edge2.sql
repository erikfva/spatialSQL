CREATE OR REPLACE FUNCTION public.sicob_snap_edge2(_opt json)
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
 -------------------------------

	_listsql text[];
    db text;
    username text;
    connectstring text;
    conn text;
    _max_connections integer;
    
    dispatch_result integer;
    dispatch_error text;
    n integer;
    n_tasks integer;
    pending_count integer;
    workers_count integer;

    status integer;
    n_process integer;
    processes text[];
    result jsonb;
    results jsonb = '[]';


BEGIN
---------------------------
--PRE - CONDICIONES
---------------------------
--> Los elementos de las capas de entradas deben ser poligonos simples (POLYGON). No deben ser Multipolygon o colecciones.
--> El campo identificador unico de cada capa debe ser "sicob_id".
---------------------------
--PARAMETROS DE ENTRADA
---------------------------
--> a : capa de poligonos cuyos bordes se ajustaran a los bordes de la capa "b".
--> condition_a (opcional): Filtro para los datos de "a". Si no se especifica, se toman todos los registros.
--> b : capa de poligonos a cuyos bordes se ajustaran los poligonos de "a".
--> condition_b (opcional): Filtro para los datos de "b". Si no se especifica, se toman todos los registros.
--> subfix (opcional): texto adicional que se agregará al nombre de "a" para formar el nombre de la capa resultante. Si no se especifica por defecto es "_adjusted".
--> schema (opcional): esquema del la BD donde se creará la capa resultante. Si no se especifica se creará en "temp".
--> tolerance (opcional): Distancia máxima (en metros) para el autoajuste automático de los bordes de "a" hacia los bordes de "b" (snapping). Si no se especifica, no se realiza autoajuste y la funcion devolvera "a". 
--> add_geoinfo (opcional true/false): Agrega o no la información del código de proyección UTM 'sicob_utm', superficie (ha) 'sicob_sup' y la geometría en proyección webmercator 'the_geom_webmercator' para cada polígono. Si no se especifica NO se agrega.
--> temp (opcional true/false) : Indica si la capa resultante será temporal mientras dura la transacción. Esto se requiere cuando el resultado es utilizado como capa intermedia en otros procesos dentro de la misma transacción. Por defecto es FALSE.


---------------------------
--VALORES DEVUELTOS
---------------------------
--> lyr_adjusted : capa resultante del ajuste de bordes. Solo se incluyen dos campos: sicob_id,the_geom.


_opt := '{"a":"processed.f20171005fgcbdae84fb9ea1_nsi","b":"coberturas.parcelas_tituladas","subfix":"_adjusted","tolerance":"5.3", "schema":"temp"}';

a := (_opt->>'a')::TEXT;
_condition_a := COALESCE((_opt->>'condition_a')::text, 'TRUE');

b := (_opt->>'b')::TEXT;
_condition_b := COALESCE((_opt->>'condition_b')::text, 'TRUE');

_subfixresult := COALESCE(_opt->>'subfix','_adjusted'); 
_schema := COALESCE(_opt->>'schema','temp');
_tolerance := COALESCE((_opt->>'tolerance')::real, 0);


SELECT * FROM sicob_split_table_name(a::text) INTO sch_nameA, tbl_nameA;
SELECT * FROM sicob_split_table_name(b::text) INTO sch_nameB, tbl_nameB;
RAISE NOTICE 'sicob_snap_edge2: %', _opt::text;

    IF _tolerance > 0 THEN
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
        IF row_cnt > 0 THEN
            sql := 'DROP INDEX IF EXISTS ' || a__b || '_a; DROP INDEX IF EXISTS ' || a__b || '_b;';
            RAISE DEBUG 'Running %', sql;
            EXECUTE sql;
                    
            sql := 'CREATE INDEX ' || a__b || '_a
                    ON ' || a__b || ' 
                    USING btree (id_a); 
                    CREATE INDEX ' || a__b || '_b
                    ON ' || a__b || ' 
                    USING btree (id_b);';
            RAISE DEBUG 'Running %', sql;
            EXECUTE sql; 
        END IF;
    END IF;

    IF _tolerance = 0 OR row_cnt = 0 THEN
    --> Retornar sin hacer ajustes.
    	sql := '';
        IF _condition_a <> 'TRUE' THEN
        	sql := 'SELECT a.sicob_id, a.the_geom FROM ' || a || ' a WHERE ' || _condition_a;
        END IF;
	ELSE
        -----------------------------------------
        --AJUSTANDO LOS BORDES DE a HACIA b 
        --EN UNA DISTANCIA APROXIMADA A _tolerance (en metros)
        -----------------------------------------
           
        sql := '
        	SELECT DISTINCT inters.id_a as sicob_id, a.the_geom
            FROM ' || a__b || ' inters 
            INNER JOIN ' || a::text || ' a ON (a.sicob_id = inters.id_a)
            INNER JOIN ' || b::text || ' b ON (b.sicob_id = inters.id_b)
            WHERE (ST_CoveredBy(a.the_geom,b.the_geom) = TRUE )';
            
        --EXECUTE 'CREATE TEMPORARY TABLE poly_fullcovered ON COMMIT DROP AS ' || sql;
        EXECUTE 'DROP TABLE IF EXISTS temp.' || tbl_nameA || '_fullcovered'; 
        EXECUTE 'CREATE UNLOGGED TABLE temp.' || tbl_nameA || '_fullcovered AS ' || sql;  
        sql := '
              SELECT DISTINCT inters.id_a as sicob_id,
              (SELECT the_geom FROM ' || a::text || ' a WHERE a.sicob_id = inters.id_a) as the_geom,
              (SELECT ST_Collect(b.the_geom) FROM ' || a__b || ' a__b INNER JOIN ' || b::text || ' b ON(a__b.id_a = inters.id_a AND b.sicob_id = a__b.id_b) ) as target
              FROM ' || a__b || ' inters
              WHERE NOT EXISTS (SELECT sicob_id FROM temp.' || tbl_nameA || '_fullcovered t WHERE t.sicob_id = inters.id_a) 
              ';
        EXECUTE 'DROP TABLE IF EXISTS temp.' || tbl_nameA || '_partialcovered'; 
        EXECUTE 'CREATE UNLOGGED TABLE temp.' || tbl_nameA || '_partialcovered AS ' || sql;     
        
        sql := '
            SELECT 
                sicob_id, 
                CASE WHEN ST_NRings(the_geom) > 1 THEN
                        sicob_update_exteriorring( 
                            sicob_snap_edge(
                                st_exteriorring(the_geom),
                                target,
                                ''{"tolerance":"' || _tolerance::text || '"}''
                            ), 
                            the_geom, 
                            ''{}''
                        )
                ELSE
                        sicob_snap_edge(
                            st_exteriorring(the_geom),
                            target,
                            ''{"tolerance":"' || _tolerance::text || '","returnpolygon":true}''
                        )

                END
                AS the_geom
            FROM temp.' || tbl_nameA || '_partialcovered a
              '; 
---------------------------------------------     
IF TRUE THEN         
BEGIN
        sql := '
            SELECT 
                sicob_id, 
                CASE WHEN ST_NRings(the_geom) > 1 THEN
                        sicob_update_exteriorring( 
                            sicob_snap_edge(
                                st_exteriorring(the_geom),
                                target,
                                ''{"tolerance":"' || _tolerance::text || '"}''
                            ), 
                            the_geom, 
                            ''{}''
                        )
                ELSE
                        sicob_snap_edge(
                            st_exteriorring(the_geom),
                            target,
                            ''{"tolerance":"' || _tolerance::text || '","returnpolygon":true}''
                        )
                END
                AS the_geom
            FROM temp.' || tbl_nameA || '_partialcovered a
            WHERE <chunk_condition>
              ';  
    SELECT sicob_splitsql(sql,('{"table_to_chunk":"temp.' || tbl_nameA || '_partialcovered", "partitions":"10"}')::json) INTO _listsql;
    
	SELECT current_database(),CURRENT_USER INTO db,username;
	connectstring := QUOTE_LITERAL('password=Abt2016! dbname=' || db || ' user=' || username);
	--RAISE NOTICE '%', connectstring;
  _max_connections := COALESCE( (_opt->>'max_connections')::int, 2);
  
  n_tasks :=  array_upper(_listsql, 1); 
  pending_count := n_tasks; 
  workers_count := 0;
  n := 0; 
  n_process := 1;  
  -- loop through listsql
  LOOP
      IF pending_count = 0 THEN
          EXIT;
      END IF;
      
      LOOP
          IF workers_count = _max_connections OR n = n_tasks THEN
              EXIT;
          END IF;
          
          n := n + 1;

          conn := 'p_' || n;       
          sql := 'SELECT dblink_connect(' || QUOTE_LITERAL(conn) || ',' || connectstring || ');';
          BEGIN
              EXECUTE sql;
          EXCEPTION WHEN others THEN
              IF SQLSTATE = '42710' THEN --> duplicate connection name
                  sql := 'SELECT dblink_disconnect(' || QUOTE_LITERAL(conn) || ');';
                  EXECUTE sql;
                  sql := 'SELECT dblink_connect(' || QUOTE_LITERAL(conn) || ',' || connectstring || ');';
                  EXECUTE sql;
              ELSE 
                  RAISE EXCEPTION 'sicob_paralellSQL: %, %', SQLERRM, SQLSTATE;
              END IF;
          END;    
            
          SELECT array_append(processes, n::text) INTO processes;    
          sql := replace(_listsql[n],'\"','"');
          SELECT dblink_cancel_query(conn);         
          --send the query asynchronously using the dblink connection
          sql := 'SELECT dblink_send_query(' || QUOTE_LITERAL(conn) || ',' || QUOTE_LITERAL(sql) || ');';
          EXECUTE sql INTO dispatch_result;

          -- check for errors dispatching the query
          IF dispatch_result = 0 THEN
              sql := 'SELECT dblink_error_message(' || QUOTE_LITERAL(conn)  || ');';
              EXECUTE sql INTO dispatch_error;
              RAISE '%', dispatch_error;
          END IF; 
          workers_count := workers_count + 1;         
      END LOOP;
      
      LOOP
          IF pending_count = 0 OR (n < n_tasks and workers_count < _max_connections)  THEN
              EXIT;
          END IF;
          
          conn := 'p_' || processes[n_process];
          sql := 'SELECT dblink_is_busy(' || QUOTE_LITERAL(conn) || ');';
          EXECUTE sql INTO status;
          
          IF status = 0 THEN	        	
              -- check for error messages
              sql := 'SELECT dblink_error_message(' || QUOTE_LITERAL(conn)  || ');';
              EXECUTE sql INTO dispatch_error;
          
              IF dispatch_error <> 'OK' THEN
                  -- show error and finish
                  RAISE '%', dispatch_error;
              END IF;
              
              --get results
              sql := '
              SELECT * FROM dblink_get_result(' || QUOTE_LITERAL(conn) || ' ) AS t(res jsonb);
              ';
              EXECUTE sql INTO result;
              
              --result := '{"msg":"OK"}'::json;
                                    
              result := jsonb_build_object('listsql_id', processes[n_process], 'result', result);
              --RAISE NOTICE 'Completado %', conn;
              
              results := results || jsonb_build_array(result);
              
              --SELECT array_append(results, result::json ) INTO results;            
              
              -- finish process
              sql := 'SELECT dblink_disconnect(' || QUOTE_LITERAL(conn) || ');';
              EXECUTE sql;        	
              SELECT array_remove(processes, processes[n_process]) INTO processes;
                          
              IF n_process > array_length(processes, 1) THEN
                  n_process = 1;
              END IF;  
              
              workers_count := workers_count - 1;
              pending_count := pending_count - 1;          
          ELSE
              IF n_process = array_length(processes, 1) THEN
                  n_process := 1;
              ELSE
                  n_process := n_process + 1;
              END IF;
          END if;
          
          
      END LOOP;
      
  END LOOP;
 
 
RETURN results;
    
-- error catching to disconnect dblink connections, if error occurs
EXCEPTION WHEN others THEN
  BEGIN
  	--RAISE NOTICE '% %', SQLERRM, SQLSTATE;
    IF array_length(processes, 1) > 0 THEN
      FOREACH conn IN ARRAY processes LOOP
          sql := 'SELECT dblink_disconnect(p_' || conn || ');';
          EXECUTE sql;
      END LOOP;
    END IF;
  	RAISE EXCEPTION 'sicob_paralellSQL: %, %', SQLERRM, SQLSTATE;
  EXCEPTION WHEN others THEN
    RAISE EXCEPTION 'sicob_paralellSQL: %, %', SQLERRM, SQLSTATE;
  END;	
END;              

END IF;
              
---------------------------------------------              
--       EXECUTE 'DROP TABLE IF EXISTS temp.' || tbl_nameA || '_snapping'; 
--        EXECUTE 'CREATE UNLOGGED TABLE temp.' || tbl_nameA || '_snapping AS ' || sql;
   
        sql := '       
          SELECT t.* 
          FROM 
          (
              SELECT sicob_id,the_geom 
              FROM ' || a::text || ' a 
              WHERE NOT EXISTS (SELECT sicob_id FROM ' || a__b || ' a__b WHERE a__b.id_a = a.sicob_id)
              UNION ALL
              SELECT * FROM temp.' || tbl_nameA || '_fullcovered
              UNION ALL
              SELECT * FROM temp.' || tbl_nameA || '_snapping
          )
          t ORDER BY t.sicob_id    
        ';   
        RAISE DEBUG 'Running %', sql;  
    END IF; 


    
    IF sql = '' THEN
    	--> Si no se genero una nueva cobertura devuelve la misma de entrada.
    	RETURN ('{"lyr_adjusted":"' || a || '"}')::json;
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
    
    EXECUTE 'ALTER TABLE ' || __a || ' ADD PRIMARY KEY (sicob_id);';
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
    
    RETURN ('{"lyr_adjusted":"' || __a || '"}')::json;
    

EXCEPTION
WHEN others THEN
            RAISE EXCEPTION 'geoSICOB (sicob_snap_edge) % , % , _opt: % | sql: %', SQLERRM, SQLSTATE, _opt, sql;	
END;
$function$
 