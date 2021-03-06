SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_update_exteriorring(newring geometry, p geometry, _opt json)
 RETURNS geometry
 LANGUAGE sql
 IMMUTABLE
AS $function$
SELECT ST_BuildArea(ST_Collect(b.final_geom)) AS filtered_geom
  FROM (SELECT ST_MakePolygon((/* Get outer ring of polygon */
    newring /* ie the outer ring */
    ),  ARRAY(/* Get all inner rings > a particular area */
     SELECT ST_ExteriorRing(b.geom) AS inner_ring
       FROM (SELECT (ST_DumpRings(a.the_geom)).*) b
      WHERE b.path[1] > 0 /* ie not the outer ring */
    ) ) AS final_geom
         FROM (SELECT ST_GeometryN(ST_Multi($2),/*ST_Multi converts any Single Polygons to MultiPolygons */
                                   generate_series(1,ST_NumGeometries(ST_Multi($2)))
                                   ) AS the_geom
               ) a
       ) b
$function$
 