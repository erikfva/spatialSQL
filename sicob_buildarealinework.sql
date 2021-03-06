SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_buildarealinework(linework geometry)
 RETURNS geometry
 LANGUAGE sql
 IMMUTABLE
AS $function$WITH data AS (SELECT * FROM ST_Dump(ST_UnaryUnion($1)))
SELECT ST_SetSRID(ST_BuildArea(ST_Collect(geom)), ST_SRID($1))
FROM
( -- This is the noded linework, broken up where they cross each other
  SELECT (ST_Dump(ST_Union(geom))).geom FROM data
) t1,
( -- This is a [MULTI]POINT of any crossings or closures
  SELECT ST_Union(geom) AS pt
  FROM (
    SELECT ST_Intersection(A.geom, B.geom) AS geom
    FROM data A, data B
    WHERE A.path[1] < B.path[1] AND ST_Intersects(A.geom, B.geom)
    UNION SELECT ST_StartPoint(geom) FROM data WHERE ST_IsClosed(geom)
  ) s
) t2
WHERE ST_Intersects(ST_StartPoint(geom), pt) AND ST_Intersects(ST_EndPoint(geom), pt);$function$
 