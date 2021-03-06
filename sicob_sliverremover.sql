SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_sliverremover(geometry)
 RETURNS geometry
 LANGUAGE sql
 STRICT
AS $function$
SELECT 
  st_buffer( 
    	st_buffer( st_buildarea($1) ,-0.00000000000001,'endcap=flat join=bevel'),
    	0.00000000000001,'endcap=flat join=bevel'
 ) as the_geom;
$function$
 