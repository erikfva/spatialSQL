SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_scale(geoma geometry, xfactor double precision, yfactor double precision)
 RETURNS geometry
 LANGUAGE plpgsql
AS $function$
BEGIN
  RETURN (
      SELECT
      ST_Translate(
          ST_Scale(geomA, XFactor, YFactor),
          ST_X(ST_Centroid(geomA))*(1 - XFactor),
          ST_Y(ST_Centroid(geomA))*(1 - YFactor)
      ) AS scaled_geometry
  );
END;
$function$
 