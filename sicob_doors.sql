SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_doors(_edge geometry, _tolerance double precision)
 RETURNS geometry
 LANGUAGE plpgsql
 STRICT
AS $function$
DECLARE 
    _buffer2snap geometry;
    _sql text;
    _newedge geometry;
    row_cnt integer;
BEGIN

WITH
	dp_lines AS (
		SELECT st_dump( _edge )  as dp 
	),
   segments as (
     SELECT row_number() over() as gid,
              (dp).geom as the_geom 
     FROM dp_lines
   ),  
	pts AS ( 
		SELECT row_number() over() As id,
			the_geom
		FROM
 			( 
            	SELECT st_startpoint(the_geom) as the_geom FROM  segments
                union all 
                SELECT st_endpoint(the_geom) as the_geom FROM  segments
            )t
	),
    vertex AS (
		SELECT id, the_geom,
		(SELECT count(gid) FROM segments s WHERE  st_intersects(s.the_geom, st_buffer(pts.the_geom, 0.0000000000001) ) ) as cntseg
		FROM pts
	),
	vertex_dangle AS (
		SELECT * FROM vertex
		WHERE cntseg = 1
	),
    doors AS (
		SELECT a.id as id1,a.the_geom as p1, b.id as id2, b.the_geom p2,
			ST_MakeLine(a.the_geom,b.the_geom) as the_geom 
		FROM vertex_dangle a, vertex_dangle b
		WHERE ST_Azimuth(a.the_geom,b.the_geom) > ST_Azimuth(b.the_geom,a.the_geom)
		and ST_Distance(a.the_geom,b.the_geom) <=  _tolerance
	)
    SELECT  st_union(the_geom)  INTO _newedge 
    FROM doors;	
    
	RETURN _newedge;     

EXCEPTION
WHEN others THEN
            RAISE EXCEPTION 'geoSICOB (sicob_doors): % (%) | _edge: % | _tolerance: %', SQLERRM, SQLSTATE, st_astext(_edge), _tolerance::text;	
END;
$function$
 