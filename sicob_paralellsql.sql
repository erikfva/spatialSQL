CREATE OR REPLACE FUNCTION public.sicob_paralellsql(_listsql text[], _opt json)
 RETURNS json
 LANGUAGE plpgsql
AS $function$
DECLARE   
    sql text;
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
 
-- _listsql := ARRAY['SELECT sicob_analisis_sobreposicion(''{"lyr_in":"uploads.f20170704gcfebdac5d7c097","doanalisys":["TPFP"]}''::json)', 'SELECT sicob_analisis_sobreposicion(''{"lyr_in":"uploads.f20170704gcfebdac5d7c097","doanalisys":["ASL"]}''::json)','SELECT sicob_analisis_sobreposicion(''{"lyr_in":"uploads.f20170704gcfebdac5d7c097","doanalisys":["ATE"]}''::json)'];
  
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
          sql := _listsql[n];
                   
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
/*  
  FOR n IN 1 .. array_upper(_listsql, 1)
	LOOP
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
               
        sql := _listsql[n];
               
        --send the query asynchronously using the dblink connection
        sql := 'SELECT dblink_send_query(' || QUOTE_LITERAL(conn) || ',' || QUOTE_LITERAL(sql) || ');';
        EXECUTE sql INTO dispatch_result;

        -- check for errors dispatching the query
        IF dispatch_result = 0 THEN
        sql := 'SELECT dblink_error_message(' || QUOTE_LITERAL(conn)  || ');';
        EXECUTE sql INTO dispatch_error;
            RAISE '%', dispatch_error;
        END IF;
        
	END LOOP;     

	n := 1;
    -- wait until all queries are finished
	LOOP
    	conn := 'p_' || processes[n];
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
                                  
            result := jsonb_build_object('listsql_id', processes[n], 'result', result);
            --RAISE NOTICE 'Completado %', conn;
            
            results := results || jsonb_build_array(result);
            
            --SELECT array_append(results, result::json ) INTO results;            
            
            -- finish process
            sql := 'SELECT dblink_disconnect(' || QUOTE_LITERAL(conn) || ');';
            EXECUTE sql;        	
            SELECT array_remove(processes, processes[n]) INTO processes;
                        
            IF n > array_length(processes, 1) THEN
            	n = 1;
            END IF;            
        ELSE
        	IF n = array_length(processes, 1) THEN
            	n := 1;
            ELSE
            	n := n + 1;
            END IF;
      	END if;
        
        IF COALESCE(array_length(processes, 1),0) = 0 THEN
      		EXIT;
    	END IF;
	END LOOP;
 */   
 
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
$function$
 