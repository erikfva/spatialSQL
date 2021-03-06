SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_polygon_to_line(geometry)
 RETURNS geometry
 LANGUAGE sql
AS $function$
SELECT ST_MakeLine(geom) FROM (
SELECT (ST_DumpPoints(ST_ExteriorRing(
(ST_Dump($1)).geom
))).geom
) AS linpoints
$function$
 