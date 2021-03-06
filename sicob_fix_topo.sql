SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_fix_topo(_opt json)
 RETURNS json
 LANGUAGE plpgsql
AS $function$
--REALIZA LA CORRECCION TOPOLOGICA DEL(LOS)POLIGONO(S)
DECLARE 
	_out json := '{}';
    lyr_fix text; 
    lyr_out text;
 	tbl_name text; sch_name text;
 	row_cnt integer;
	sql text;
    
    db text;
    username text;
    connectstring text;
    conn text;
BEGIN
	--_opt := '{"lyr_in":"uploads.f20181121adgbecf285a351e"}'::json;
   
	lyr_fix := COALESCE( (_opt->>'lyr_in')::text , '');
    SELECT * FROM sicob_split_table_name(lyr_fix) INTO sch_name, tbl_name;
    _out := ('{"lyr_in":"' || lyr_fix || '"}')::json;

--> 1.- Verificar si el layer es valido
/*
	sql := Format('
	SELECT ST_IsValidReason(the_geom) 
	FROM %s 
	WHERE ST_IsValid(the_geom) = FALSE;
    ', lyr_fix);
	RAISE DEBUG 'Running %', sql;
	EXECUTE sql;
	GET DIAGNOSTICS row_cnt = ROW_COUNT;
    
    IF row_cnt > 0 THEN
    	--RAISE EXCEPTION 'Existen % elementos no validos.',row_cnt;
        PERFORM sicob_makevalid(lyr_fix);
    END IF;
*/
	PERFORM sicob_makevalid(lyr_fix);
	
    IF( COALESCE((_opt->>'check')::boolean, FALSE) = TRUE ) THEN
--> 2.- Verificar si existen posibles errores topologicos
		RAISE NOTICE 'Verificando porque se indico.';
      sql := Format('
     with 
      dumped AS(
        select 
        (
            st_dumprings(
              (st_dump(
                ST_union(the_geom) 
              )).geom
            ) 
        ) as dp
        FROM %s
      )
      select
          count(*)
      from 
          dumped
      where
          (dp).path[1] > 0 --> que no sea un hueco del poligono
          AND
          trunc( st_area( (dp).geom ) * 10000000000 ) = 0; --> sin superficie
      ', lyr_fix);
      RAISE DEBUG 'Running %', sql;
      EXECUTE sql INTO row_cnt;
      
      IF row_cnt = 0 THEN --> No existen posibles errores.
          RETURN _out;
      END IF;
	END IF;
--> Creando una copia del layer.	
	lyr_out := 'processed.' || tbl_name || '_fixt';
    EXECUTE 'DROP TABLE IF EXISTS ' || lyr_out;
    sql := Format('
    	CREATE TABLE %s AS SELECT sicob_id,%s,the_geom ' ||
        CASE WHEN SICOB_exist_column(lyr_fix, 'the_geom_webmercator') THEN
        	', the_geom_webmercator' 
        ELSE
        	''
        END || 
        ' FROM %s t;', lyr_out, 
        sicob_no_geo_column(lyr_fix,'{sicob_id, gid}','t.'),
        lyr_fix);
 	RAISE NOTICE 'Running %', sql;
	EXECUTE sql;
    EXECUTE 'ALTER TABLE ' || lyr_out || ' ADD PRIMARY KEY (sicob_id);';

-- FIX: ADD SEQUENCE FOR sicob_id 20/02/2019
    sql := Format('
    create sequence %s_id_seq
	owned by %s.sicob_id;
    ', lyr_out, lyr_out);
    EXECUTE sql;
    sql := Format('
    SELECT setval(''%s_id_seq'', 
    	(SELECT max(sicob_id) FROM %s)
    )', lyr_out, lyr_out);
    EXECUTE sql;
    sql := Format('
    ALTER TABLE %s
    ALTER COLUMN sicob_id SET DEFAULT nextval(''%s_id_seq''::regclass);
	',lyr_out, lyr_out);
    EXECUTE sql;
    ---------------------------------

	PERFORM Populate_Geometry_Columns(('processed.' || tbl_name || '_fixt')::regclass);
	sql := 'CREATE INDEX ' || tbl_name || '_fixt_geom_gist ON processed.' || tbl_name || '_fixt USING gist(the_geom)';
	EXECUTE sql; 

-->7.- Eliminar topologia existente (en caso de que se haya corregido
--		con anterioridad). 
	BEGIN
		PERFORM topology.DropTopology('fix_' || tbl_name);
    EXCEPTION
    	WHEN others THEN
    	RAISE NOTICE 'geoVision [sicob_fix_topo(%)]: No existe la topologia %',_opt, 'fix_' || tbl_name;	
    END;
    

-->3.- Crear la topologia.
	PERFORM topology.CreateTopology('fix_' || tbl_name, 4326, 0.000001, FALSE);
-->4. Crear el campo topologico en el layer y relacionarlo con la topologia creada
	row_cnt := topology.AddTopoGeometryColumn('fix_' || tbl_name,'processed', tbl_name || '_fixt', 'topogeom', 'POLYGON');
-->5.- Convertir poligono a topologia
	sql := Format('
    UPDATE processed.%s_fixt SET topogeom = topology.toTopoGeom(the_geom,''fix_%s'',%s,0.000001);
    ',tbl_name,tbl_name,row_cnt);
  	RAISE DEBUG 'Running %', sql;
	EXECUTE sql;
-->6.- Reasignar topologia
	sql := Format('
	UPDATE processed.%s_fixt SET the_geom = topogeom;
    ', tbl_name);  
  	RAISE DEBUG 'Running %', sql;
	EXECUTE sql;
	
-->7.- Eliminar topologia 
--	PERFORM topology.DropTopology('fix_' || tbl_name);
/*
SELECT current_database(),CURRENT_USER INTO db,username;
connectstring := QUOTE_LITERAL('password=Abt2018* dbname=' || db || ' user=' || username);    
conn := sicob_dblink_connect('');
sql := 'select pg_sleep(10),topology.DropTopology(''fix_' || tbl_name || ''')';
PERFORM dblink_send_query(conn, sql);
*/
	-- Enviando al geosicobTask mediante  el canal "gvcmd", la orden de eliminar
    -- la topologia despues de 10 segundos para evitar error debido a que todavia
    -- esta en uso.
    sql := 'select pg_sleep(10),topology.DropTopology(''fix_' || tbl_name || ''')';
    PERFORM pg_notify('gvcmd', json_build_object(
    	'cmd', 'executeSQL',
        'sql', sql
    	)::text
    );
       
    _out := _out::jsonb || ('{"lyr_fix":"processed.' || tbl_name || '_fixt"}')::jsonb;
	RETURN _out;

	EXCEPTION
	WHEN others THEN
    	RAISE EXCEPTION 'geoVision [sicob_fix_topo(%)]: % (%)', _opt , SQLERRM, SQLSTATE;	
END;
$function$
 