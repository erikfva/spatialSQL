CREATE OR REPLACE FUNCTION public.sicob_testfunc2(reloid regclass)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE 
 sql text;
 tp text; tt text;
 tbl_name text; sch_name text;
 row_cnt integer;
BEGIN
--reloid := 'processed.f20160523bdcgfae4403184a_nsi';
SELECT * FROM sicob_split_table_name(reloid) INTO sch_name, tbl_name;

sql := 'DROP TABLE IF EXISTS temp.' || tbl_name || '_tit';
RAISE DEBUG 'Running %', sql;
EXECUTE sql;

sql := 'SELECT 
  b.sicob_id,
  (ST_Dump(st_intersection(a.the_geom, b.the_geom))).geom AS the_geom,
  a.parcela AS nombre,
  a.numerodocu AS titulo,
  a.codcat as codcatastral,
  a.nombreplan AS propietario
FROM
  ' || reloid::text || ' b,
  coberturas.predios_titulados_z20_geo_26012016 a
WHERE
  st_intersects(a.the_geom, b.the_geom)';
  
RAISE DEBUG 'Running %', sql;
EXECUTE 'CREATE TABLE ' || 'temp.' || tbl_name || '_tit AS ' || sql;
GET DIAGNOSTICS row_cnt = ROW_COUNT;
--PERFORM sicob_add_geoinfo_column('temp.' || tbl_name || '_tit');
--PERFORM sicob_update_geoinfo_column('temp.' || tbl_name || '_tit');

sql := 'DROP TABLE IF EXISTS processed.' || tbl_name || '_pcla';
RAISE DEBUG 'Running %', sql;
EXECUTE sql;

IF row_cnt = 0 THEN
	RAISE DEBUG 'Tabla: % vacia.','temp.' || tbl_name || '_tit';
    sql := 'SELECT sicob_id,the_geom,'''' AS nombre, '''' AS titulo, '''' AS codcatastral, '''' AS propietario, the_geom_webmercator, sicob_sup from ' || reloid::text;
ELSE
	sql := 'DROP TABLE IF EXISTS temp.' || tbl_name || '_notit';
	RAISE DEBUG 'Running %', sql;
	EXECUTE sql;
	
    sql := 'SELECT a.sicob_id, (ST_Dump( COALESCE(ST_Difference(the_geom, (SELECT ST_Union( b.the_geom ) 
                                         FROM  temp.' || tbl_name  || '_tit b
                                         WHERE ST_Intersects(a.the_geom, b.the_geom)
                                         )), a.the_geom) 
                                         )).geom as the_geom,
  		''''::varchar as nombre,
  		''''::varchar as titulo,
  		''''::varchar as codcatastral,
  		''''::varchar as propietario
		FROM ' || reloid::text || ' a';
    RAISE DEBUG 'Running %', sql;
	EXECUTE 'CREATE TABLE temp.' || tbl_name || '_notit AS ' || sql;
    sql := 'SELECT * FROM temp.' || tbl_name || '_tit UNION SELECT * FROM temp.' || tbl_name || '_notit';
	--PERFORM sicob_add_geoinfo_column('temp.' || tbl_name || '_notit');
	--PERFORM sicob_update_geoinfo_column('temp.' || tbl_name || '_notit');    
    --sql := 'SELECT * FROM temp.' || tbl_name || '_tit    
END IF;

RAISE DEBUG 'Running %', sql;
EXECUTE 'CREATE TABLE ' || 'processed.' || tbl_name || '_pcla AS (' || sql || ')';

PERFORM sicob_add_geoinfo_column('processed.' || tbl_name || '_pcla');
PERFORM sicob_update_geoinfo_column('processed.' || tbl_name || '_pcla');

sql := 'DELETE FROM processed.' || tbl_name || '_pcla WHERE sicob_sup = 0';
RAISE DEBUG 'Running %', sql;
EXECUTE sql;

EXCEPTION
WHEN others THEN
            RAISE EXCEPTION 'geoSICOB % (func2): % (%)', reloid, SQLERRM, SQLSTATE;	
END;
$function$