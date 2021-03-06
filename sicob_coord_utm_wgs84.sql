SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_coord_utm_wgs84(_opt json)
 RETURNS TABLE(sicob_id integer, elem integer, punto integer, xcoord double precision, ycoord double precision, sicob_utm integer)
 LANGUAGE plpgsql
 STABLE
AS $function$
DECLARE
sql text;

BEGIN
--PARAMETROS EN VARIABLE _opt
  --lyr_in, <-- capa PostGIS con los elementos geometricos
  --condition TEXT  <-- (opcional) condicion para el filtro WHERE
  sql := '
  SELECT
      sicob_id::int,
      ((dp).path[1])::int as elem,
      COALESCE((dp).path[3],(dp).path[2]) as punto,
      st_x((dp).geom) as xcoord,
      st_y((dp).geom) as ycoord,
      sicob_utm::int
  FROM (
      SELECT 
          sicob_id,
          sicob_utm,
          st_dumppoints(st_transform(the_geom, sicob_utm)) as dp
      FROM (
          SELECT 
              ' || COALESCE( sicob_feature_id((_opt->>'lyr_in')::text) ,'sicob_id') || ' AS sicob_id,
              CASE WHEN exist_sicob_utm AND COALESCE(sicob_utm::int, 0) > 0 THEN 
                  CAST(sicob_utm AS integer)
              ELSE
                  sicob_utmzone_wgs84(the_geom)
              END AS sicob_utm,
              the_geom
          FROM ' || (_opt->>'lyr_in')::text || '
              CROSS  JOIN sicob_exist_column(''' || (_opt->>'lyr_in')::text || ''', ''sicob_utm'') AS sicob_utm(exist_sicob_utm) ' ||
          COALESCE( 'WHERE (' ||  (_opt->>'condition')::text || ') ', '') || '
          LIMIT 1000 --> LIMITANDO LA CANTIDAD DE POLIGONOS PARA EVITAR PROCESOS LARGOS!!!
      ) t
   ) u';
  RETURN QUERY EXECUTE sql;
	EXCEPTION
	WHEN others THEN
            RAISE EXCEPTION 'geoSICOB (sicob_coord_utm_wgs84); _opt:%, error:(%,%)', _opt::text, SQLERRM,SQLSTATE;	
END;
$function$
 