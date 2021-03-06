SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_snap_line2line_old(line_a geometry, line_b geometry)
 RETURNS geometry
 LANGUAGE plpgsql
AS $function$
DECLARE 
 new_borde geometry;
BEGIN
/*
SELECT the_geom into poly_a FROM processed.f20160708agfbdecf223329c_nsi
    WHERE sicob_id = 3;

with 
b_into_2snap AS ( --Obteniendo la parte de b que se encuentra dentro de temp._borde2snap
  SELECT idpol_b, row_number() over() as id, (dmp).geom as the_geom
  from(
      SELECT idpol_b, st_dump(the_geom) as dmp  
      FROM (
          SELECT	b.sicob_id as idpol_b,
          CASE 
              WHEN ST_CoveredBy(b.the_geom, a.the_geom) 
              THEN b.the_geom 
          ELSE 
              ST_Multi(
                  ST_Intersection(a.the_geom,b.the_geom)
              ) 
          END AS the_geom 
          FROM coberturas.predios_titulados b, 
          temp._borde2snap a 
          where ST_Intersects(b.the_geom,a.the_geom)  AND NOT ST_Touches(b.the_geom, a.the_geom)    
      ) q
  ) p
),
pol_b as(
	SELECT the_geom FROM b_into_2snap
    WHERE idpol_b = 70160 --702917 181038 
)
SELECT the_geom into poly_b FROM pol_b;
*/

WITH
segmentb AS (
	SELECT row_number() over() as id_segment, (dp).geom as the_geom, st_startpoint((dp).geom) as pini, st_endpoint((dp).geom) as pfin
    FROM( 
		SELECT st_dump(line_b) as dp
    ) t
),  
ptob AS
(
	SELECT id_segment, row_number() over () as vert,
    CASE WHEN st_equals((dmp).geom,pini) THEN 'i'
    	 WHEN st_equals((dmp).geom,pfin) THEN 'f'
         ELSE 'm'
    END
     as pos,  (dmp).geom as the_geom
    FROM
    (
    	SELECT id_segment, pini, pfin, st_dumppoints(the_geom) as dmp
        FROM segmentb
	) p
),
ptoa AS ( 
	SELECT DISTINCT 
    points_to_project_onto_line.id_segment as id_segmentb, 
    points_to_project_onto_line.vert vertb,
    points_to_project_onto_line.pos posb,
	ST_ClosestPoint(
			line_a,
			points_to_project_onto_line.the_geom
	) as the_geom
	FROM ptob points_to_project_onto_line
),
segmenta as (
SELECT id_segmentb, 
    CASE WHEN degrees(ST_Azimuth( st_startpoint(line_a) , st_startpoint(the_geom) ))
    		< degrees(ST_Azimuth( st_startpoint(line_a) , st_endpoint(the_geom) ))
         THEN  the_geom
    ELSE
    	ST_Reverse(the_geom)
    END as the_geom  
FROM(
	SELECT id_segmentb,     
    sicob_fix_line_overlap( st_makeline(the_geom) ) as the_geom
    FROM ptoa
    group by id_segmentb
    ) t
   --AQUI MEDIR DISTANCIA DEL SEGMENTO PROYECTADO PARA ELIMINAR CORTES PERPENDICULARES
),
masksegmenta as (
	SELECT  id_segmentb,
    st_buffer(
    sicob_st_linesubstring(
		line_a,
        st_startpoint(the_geom),
        st_endpoint(the_geom)    	
    )
    ,0.0000001 ) as the_geom
    from segmenta
),
clipa as (
  SELECT row_number() over() as id_segment,(dmp).geom as the_geom, 
  st_startpoint((dmp).geom)  as pini, st_endpoint((dmp).geom)  as pfin
  FROM(
     SELECT st_dump( st_difference( line_a , m.the_geom )) as dmp 
     FROM 
     (select st_union(the_geom) as the_geom FROM  masksegmenta) m
  )t
),
ends_pts_clipa as (
	SELECT segmenta as id_segment, 0 as pospto, pini as pto FROM clipa
    UNION
    SELECT segmenta as id_segment, ST_NPoints(the_geom)-1 as pospto, pfin as pto FROM clipa
),
ends_pts_segmentb as (
	SELECT id_segment, 0 as pospto, pini as pto FROM segmentb
    UNION
    SELECT id_segment, ST_NPoints(the_geom)-1 as pospto, pfin as pto FROM segmentb
),
ends_pts_dist AS(
  SELECT t1.id_segment AS id_segmenta, t1.pospto as posptoa,t2.id_segment AS id_segmentb, t2.pospto as posptob, ST_Distance(t1.pto, t2.pto) as dist
  FROM ends_pts_clipa t1, ends_pts_segmentb t2 
  WHERE ST_DWithin(t1.pto, t2.pto, 2 * 6::float/111000) 
  ORDER BY t1.id_segment, ST_Distance(t1.pto, t2.pto) ASC
),
snapinf as (
SELECT 
  t1.id_segmenta,
  t1.posptoa,
  t1.id_segmentb,
  t1.posptob,
  t1.dist
FROM
  ends_pts_dist t1
WHERE
  id_segmentb IN (SELECT t2.id_segmentb FROM ends_pts_dist t2 WHERE t2.id_segmenta = t1.id_segmenta 		
  AND t2.posptoa = t1.posptoa ORDER BY t2.dist LIMIT 1)  
AND 
t1.posptob IN (SELECT t2.posptob FROM ends_pts_dist t2 WHERE t2.id_segmenta = t1.id_segmenta AND t2.posptoa = t1.posptoa ORDER BY t2.dist LIMIT 1)
),
snapclipa1 AS(
SELECT 
  i.id_segmenta,
  st_setpoint(
  st_setpoint(c.the_geom, i.posptoa,(SELECT t.pto FROM ends_pts_segmentb t WHERE t.id_segment = i.id_segmentb AND t.pospto = i.posptob)),
  i2.posptoa,
  (SELECT t.pto FROM ends_pts_segmentb t WHERE t.id_segment = i2.id_segmentb AND t.pospto = i2.posptob))
   AS the_geom
FROM
  snapinf i
  INNER JOIN snapinf i2 ON (i.id_segmenta = i2.id_segmenta)
  INNER JOIN clipa c ON (i.id_segmenta = c.segmenta)
WHERE
  i.posptoa = 0 AND 
  i2.posptoa > 0
 ),
snapclipa2 AS(
SELECT 
  c.segmenta,
  st_setpoint(c.the_geom, i.posptoa,(SELECT t.pto FROM ends_pts_segmentb t WHERE t.id_segment = i.id_segmentb AND t.pospto = i.posptob)) AS the_geom
FROM
  clipa c
  INNER JOIN snapinf i ON (c.segmenta = i.id_segmenta)
WHERE
  i.id_segmenta NOT IN (SELECT id_segmenta FROM snapclipa1)
),
snapclipa as (
	SELECT st_union(snp1.the_geom,snp2.the_geom) as the_geom 
	FROM
	(SELECT st_union(the_geom) as the_geom FROM snapclipa1) snp1,
	(SELECT st_union(the_geom) as the_geom FROM snapclipa2) snp2
),
newborde as (
 SELECT st_union(la.the_geom,lb.the_geom) as the_geom
 FROM (SELECT the_geom FROM snapclipa) la,
 (select st_union(the_geom) as the_geom FROM segmentb) lb
)
SELECT   st_linemerge(the_geom) into new_borde  FROM newborde;

RETURN new_borde;     

EXCEPTION
WHEN others THEN
            RAISE EXCEPTION 'geoSICOB (sicob_snap_line2line): % (%) | line_a: % | line_b: %', SQLERRM, SQLSTATE, st_astext(line_a), st_astext(line_b);	
END;
$function$
 