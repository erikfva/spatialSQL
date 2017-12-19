CREATE OR REPLACE FUNCTION public.sicob_testfunc3()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE 
 sql text;
 reloid regclass;
 tp text; tt text;
 tbl_name text; sch_name text;
 poly_a geometry; 
 polyline_a geometry; polyline_b geometry;
 snapzone geometry;_2snapzone geometry;
 new_borde geometry;
 ra RECORD; rib RECORD; rb RECORD; r RECORD;
 tolerance float;
BEGIN

SELECT the_geom into poly_a FROM processed.f20160708agfbdecf223329c_nsi
    WHERE sicob_id = 3;

tolerance := 6::float/111000; /* Apróx. 6 metros */
FOR ra IN SELECT * FROM processed.f20160708agfbdecf223329c_nsi
LOOP
	poly_a := ra.the_geom;
    polyline_a := ST_ExteriorRing(poly_a);
    snapzone := st_union(ST_Buffer(polyline_a,  tolerance));
    _2snapzone := st_union(ST_Buffer(polyline_a, 2 * tolerance));
    new_borde := polyline_a;
    RAISE DEBUG 'sicob_id: %, polyline_a: %', ra.sicob_id, st_astext(polyline_a);
	DROP TABLE IF EXISTS b_into_2snap;    
    CREATE TEMPORARY TABLE b_into_2snap AS
  		SELECT idpol_b, row_number() over() as id, (dmp).geom as the_geom
  		FROM(
      		SELECT idpol_b, st_dump(st_union(the_geom)) as dmp  
      		FROM (
          		SELECT	b.sicob_id as idpol_b,
          		CASE 
              		WHEN ST_CoveredBy(b.the_geom, _2snapzone) 
              		THEN b.the_geom 
          			ELSE 
              		ST_Multi(
                  		ST_Intersection(_2snapzone,b.the_geom)
              		) 
          		END AS the_geom 
          		FROM coberturas.predios_titulados b 
          		WHERE ST_Intersects(b.the_geom,_2snapzone)  AND NOT ST_Touches(b.the_geom, _2snapzone)    
      		) q group by q.idpol_b
  		) p;
    
    FOR rib IN SELECT DISTINCT ON(idpol_b) idpol_b FROM b_into_2snap 
    LOOP
    	DROP TABLE IF EXISTS pol_b; 
    	CREATE TEMPORARY TABLE 
        pol_b as (
          SELECT the_geom FROM b_into_2snap
          WHERE idpol_b = rib.idpol_b --702917 181038
        );
        
 		DROP TABLE IF EXISTS line_pol_b; 
        CREATE TEMPORARY TABLE        
line_pol_b AS --Obteniendo las lineas del poligono pol_b dentro de la zona de snapping snapzone
(
    SELECT row_number() over() as segment, (dp).geom as the_geom, st_startpoint((dp).geom) as pini, st_endpoint((dp).geom) as pfin
    FROM(
        SELECT st_dump(
        	st_intersection(sicob_polygon_to_line((ST_Dump(pol_b.the_geom)).geom), snapzone 
          )      
        ) as dp  
        FROM pol_b
        ) q
);

     
        WITH
        ptob AS
        (
            SELECT segment, row_number() over () as vert,
            CASE WHEN st_equals((dmp).geom,pini) THEN 'i'
                 WHEN st_equals((dmp).geom,pfin) THEN 'f'
                 ELSE 'm'
            END
             as pos,  (dmp).geom as the_geom
            FROM
            (SELECT segment, st_dumppoints(the_geom) as dmp,
            st_startpoint(the_geom) as pini , st_endpoint(the_geom) as pfin
            from line_pol_b) p
        ),
        ptoa AS ( 
            SELECT DISTINCT 
            points_to_project_onto_line.segment as segmentb, 
            points_to_project_onto_line.vert vertb,
            points_to_project_onto_line.pos posb,
            ST_ClosestPoint(
                    polyline_a,
                    points_to_project_onto_line.the_geom
            ) as the_geom
            FROM ptob points_to_project_onto_line
        ),
        segmenta as (
        SELECT segmentb, 
            CASE WHEN degrees(ST_Azimuth( (SELECT st_startpoint(the_geom) FROM temp._borde) , st_startpoint(the_geom) ))
                    < degrees(ST_Azimuth( (SELECT st_startpoint(the_geom) FROM temp._borde) , st_endpoint(the_geom) ))
                 THEN  the_geom
            ELSE
                ST_Reverse(the_geom)
            END as the_geom  
        FROM(
            SELECT segmentb,     
            sicob_fix_line_overlap( st_makeline(the_geom) ) as the_geom
            FROM ptoa
            group by segmentb
            ) t
        ),
        masksegmenta as (
            SELECT  segmentb,
            st_buffer(
            sicob_st_linesubstring(
                polyline_a,
                st_startpoint(the_geom),
                st_endpoint(the_geom)    	
            )
            ,0.0000001 ) as the_geom
            from segmenta
        ),
        clipa as (
        SELECT row_number() over() as segmenta,(dmp).geom as the_geom, 
        st_startpoint((dmp).geom)  as pini, st_endpoint((dmp).geom)  as pfin
        FROM(
         SELECT st_dump( st_difference( polyline_a , m.the_geom )) as dmp 
         FROM 
         (select st_union(the_geom) as the_geom FROM  masksegmenta) m
         )t
        ),
        snapinfo as (
        SELECT * FROM (
        (SELECT 
          DISTINCT ON (pa.segmenta)  pa.segmenta,
          0 AS posa,
          CASE WHEN st_distance(pa.pini, pb.pini) < st_distance(pa.pini, pb.pfin) THEN
            pb.pini
          ELSE
            pb.pfin
          END AS pb,
          CASE WHEN st_distance(pa.pini, pb.pini) < st_distance(pa.pini, pb.pfin) THEN
            st_distance(pa.pini, pb.pini)
          ELSE
            st_distance(pa.pini, pb.pfin)
          END AS distancia,
          pb.segment as segmentb  
        FROM
          line_pol_b pb,
          clipa pa
        ORDER BY
          segmenta,
          distancia,
          segmentb
        )
        UNION ALL
        (SELECT  
          DISTINCT ON (pa.segmenta)  pa.segmenta,
          ST_NPoints(pa.the_geom)-1 AS posa,
          CASE WHEN st_distance(pa.pfin, pb.pini) < st_distance(pa.pfin, pb.pfin) THEN
            pb.pini
          ELSE
            pb.pfin
          END AS pb,
          CASE WHEN st_distance(pa.pfin, pb.pini) < st_distance(pa.pfin, pb.pfin) THEN
            st_distance(pa.pfin, pb.pini)
          ELSE
            st_distance(pa.pfin, pb.pfin)
          END AS distancia,
          pb.segment as segmentb  
        FROM
          line_pol_b pb,
          clipa pa
        ORDER BY
          segmenta,
          distancia,
          segmentb
        )
        ) t
        where t.distancia < 6::float/111000 *2
        ),
        snapclipaini as (
           SELECT  i.segmenta,
           st_setpoint( (SELECT the_geom FROM clipa where segmenta = i.segmenta),
                        i.posa,i.pb) as the_geom
            FROM  snapinfo i
            WHERE i.posa = 0   
        ),
        clipa2 as (
            SELECT * FROM snapclipaini
            UNION ALL
            SELECT segmenta,the_geom FROM clipa where segmenta NOT IN (SELECT segmenta FROM snapclipaini)
        ),
        snapclipafin as (
           SELECT  i.segmenta,
           st_setpoint( (SELECT the_geom FROM clipa2 where segmenta = i.segmenta),
                        i.posa,i.pb) as the_geom
            FROM  snapinfo i
            WHERE i.posa > 0
        ),
        snapclipa as (
            SELECT * FROM snapclipafin
            UNION ALL
            SELECT segmenta,the_geom FROM clipa2 where segmenta NOT IN (SELECT segmenta FROM snapclipafin)
        ),
        newborde as (
         SELECT st_union(la.the_geom,lb.the_geom) as the_geom
         FROM (SELECT st_union(the_geom) as the_geom FROM snapclipa) la,
         (select st_union(the_geom) as the_geom FROM line_pol_b) lb
        )
        SELECT  st_linemerge( the_geom) INTO new_borde  FROM newborde;


        RAISE DEBUG 'sicob_id: %, new_borde: %', ra.sicob_id, st_astext(new_borde);        
    END LOOP;
    
    RAISE DEBUG 'sicob_id: %, new_borde: %', ra.sicob_id, st_astext(new_borde);
END LOOP;

/*
CREATE TEMPORARY TABLE b_into_2snap AS
 --Obteniendo la parte de b que se encuentra dentro de temp._borde2snap
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
  ) p;

new_borde = ST_ExteriorRing(poly_a);
FOR r IN SELECT idpol_b, st_union(the_geom) as the_geom FROM b_into_2snap group by idpol_b
    LOOP
		new_borde = sicob_snap_polyline2polyline(new_borde  , sicob_polygon_to_line((ST_Dump(r.the_geom)).geom) );
    END LOOP;
*/

EXCEPTION
WHEN others THEN
            RAISE EXCEPTION 'poly_a: %, poly_b: % (%, %)', st_astext(poly_a), st_astext(r.the_geom), SQLERRM, SQLSTATE;	
END;
$function$
 