SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_fix_si(reloid text)
 RETURNS character varying
 LANGUAGE plpgsql
AS $function$
--REALIZA LA CORRECCION DE LA GEOMETRIA DEL POLIGONO
--CORRIGIENDO SOBREPOSICIONES CON SIGO MISMO (SI = SELF INTERSECTION)
--DISOLVIENDO LOS POLIGONOS SOBREPUESTOS
DECLARE 
 sql text;
 tp text; tt text; fld_id text;
 tbl_name text; sch_name text;
 row_cnt integer;
 aux jsonb;
BEGIN

	SELECT * FROM sicob_split_table_name(reloid) INTO sch_name, tbl_name;

	tt :=  tbl_name || '_tt'; -- 'temp.' || tbl_name || '_tt';
	tp :=  tbl_name || '_tp'; -- 'temp.' || tbl_name || '_tp'

	sql := 'DROP TABLE IF EXISTS ' || tt;

	EXECUTE sql;

	sql := Format('CREATE TEMPORARY TABLE %s ON COMMIT DROP AS SELECT (ST_Dump(geom)).geom as the_geom FROM (SELECT sicob_sliverremover(ST_Union(the_geom)) AS geom FROM %s) sq', tt, reloid::text);
--	sql := Format('CREATE TEMPORARY TABLE %s ON COMMIT DROP AS SELECT (ST_Dump(geom)).geom as the_geom FROM (SELECT the_geom AS geom FROM %s) sq', tt, reloid::text);

	RAISE DEBUG 'Running %', sql;
    
    BEGIN
		EXECUTE sql;
    EXCEPTION WHEN others THEN
    	RAISE NOTICE 'Error % ,(%)', SQLERRM, SQLSTATE;
        
        --IF SQLSTATE = 'XX000' THEN --> Error de topologia
        	sql := 'select sicob_fix_topo(''{"lyr_in":"' || reloid::text || '","check": "FALSE"}'')';
            RAISE NOTICE 'sicob_fix_si: Corrigiendo topologia mediante %', sql;
        	EXECUTE sql INTO aux;
            sql := Format('CREATE TEMPORARY TABLE %s ON COMMIT DROP AS SELECT (ST_Dump(geom)).geom as the_geom FROM (SELECT sicob_sliverremover(ST_Union(the_geom)) AS geom FROM %s) sq', tt, (aux->>'lyr_fix')::text);
            RAISE NOTICE 'Volviendo a ejecutar %', sql;
            EXECUTE sql;
        --END IF;
        
    END;
        
	GET DIAGNOSTICS row_cnt = ROW_COUNT;
    /*
	IF row_cnt <= 1 THEN --si no tiene mas de un elemento, no existe sobreposicion
		PERFORM sicob_makevalid(reloid::text);
		RETURN reloid::text;
	END IF;
    */
	PERFORM sicob_create_id_column(tt);
	sql := 'CREATE INDEX ' || tbl_name || '_tt_geom_gist ON ' || tt || ' USING gist(the_geom)';

	EXECUTE sql;
	sql := 'DROP TABLE IF EXISTS ' || tp;
	EXECUTE sql;

	sql := 'CREATE TEMPORARY TABLE ' || tp || ' ON COMMIT DROP AS select sicob_id, ST_PointOnSurface(the_geom) as the_geom from ' || tt;
	RAISE DEBUG 'Running %', sql;

	EXECUTE sql;

	--PERFORM sicob_create_id_column(tp);
	sql := 'CREATE INDEX ' || tbl_name || '_tp_geom_gist ON ' || tp || ' USING gist(the_geom)';
	EXECUTE sql;
    fld_id := COALESCE(sicob_feature_id(reloid),'sicob_id');
	EXECUTE 'DROP TABLE IF EXISTS ' || 'processed.' || tbl_name || '_nsi';
	-- nsi: not self intersect
	sql := 'SELECT q.*,p.the_geom FROM (SELECT punto.sicob_id, ' || sicob_no_geo_column(reloid,'{sicob_id, gid}','poly.') || ' 
	from  ' || tp ||' punto,  ' || reloid::text || ' poly
	where poly.' || fld_id || ' = (
		SELECT ' || reloid::text || '.' || fld_id || '
		FROM
  	' || tp || '
		INNER JOIN ' || reloid::text || ' 
		ON (ST_Intersects(' || tp || '.the_geom, ' || reloid::text || '.the_geom)
		and ' || tp || '.sicob_id = punto.sicob_id
	) limit 1
	)
	order by punto.sicob_id) q, ' || tt || ' p
	WHERE
	p.sicob_id = q.sicob_id';
	RAISE DEBUG 'Running %', sql;
	EXECUTE 'CREATE TABLE ' || 'processed.' || tbl_name || '_nsi AS ' || sql;
    EXECUTE 'ALTER TABLE ' || 'processed.' || tbl_name || '_nsi ADD PRIMARY KEY (sicob_id);';
	PERFORM Populate_Geometry_Columns(('processed.' || tbl_name || '_nsi')::regclass);
	sql := 'CREATE INDEX ' || tbl_name || '_nsi_geom_gist ON processed.' || tbl_name || '_nsi USING gist(the_geom)';
	EXECUTE sql; 
	PERFORM sicob_makevalid('processed.' || tbl_name || '_nsi');
	--PERFORM UpdateGeometrySRID('processed', tbl_name || '_nsi', 'the_geom', 4326);
	PERFORM  sicob_add_geoinfo_column('processed.' || tbl_name || '_nsi');
	PERFORM  sicob_update_geoinfo_column('processed.' || tbl_name || '_nsi');

	RETURN  'processed.' || tbl_name || '_nsi';
	EXCEPTION
	WHEN others THEN
		RAISE EXCEPTION 'geoSICOB [sicob_fix_si(%)]: % (%)', reloid, SQLERRM, SQLSTATE;	
END;
$function$
;CREATE OR REPLACE FUNCTION public.sicob_fix_si(_in json)
 RETURNS json
 LANGUAGE plpgsql
AS $function$
--REALIZA LA CORRECCION DE LA GEOMETRIA DEL POLIGONO
--CORRIGIENDO SOBREPOSICIONES CON SIGO MISMO (SI = SELF INTERSECTION)
--DISOLVIENDO LOS POLIGONOS SOBREPUESTOS
DECLARE 
	_out json := '{}';
    lyr_fix text; 
BEGIN

	lyr_fix := sicob_fix_si(COALESCE( (_in->>'lyr_in')::text , ''));
    _out := ('{"lyr_fix_si":"' || lyr_fix || '"}')::json;
    IF COALESCE( (_in->>'idgeoproceso')::int,0) > 0 THEN --> Si la llamada esta asociada a un geoproceso.       
        PERFORM sicob_resultado_geoproceso( ('{"idgeoproceso":"' || (_in->>'idgeoproceso')::text || '", "exec_point":"fin","msg":' || _out || '}')::json );
	END IF;  
	RETURN _out;

	EXCEPTION
	WHEN others THEN
		IF COALESCE( (_in->>'idgeoproceso')::int,0) > 0 THEN --> Si la llamada esta asociada a un geoproceso.   
        	PERFORM sicob_resultado_geoproceso( ('{"idgeoproceso":"' || (_in->>'idgeoproceso')::text || '", "exec_point":"fin","msg":{"error":"' || SQLERRM || '"}}')::json );
        END IF;
    	RAISE EXCEPTION 'geoSICOB [sicob_fix_si(%)]: % (%)', _in , SQLERRM, SQLSTATE;	
END;
$function$
 