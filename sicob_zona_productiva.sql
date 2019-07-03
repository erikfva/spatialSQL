SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_zona_productiva(_opt json)
 RETURNS TABLE(id integer, x double precision, y double precision, zonautm integer, region text)
 LANGUAGE plpgsql
AS $function$
DECLARE
sql text;
data text;
zona integer;
BEGIN
--PARAMETROS EN VARIABLE _opt
  --lyr_in, <-- capa PostGIS con los elementos geometricos
  --condition TEXT  <-- (opcional) condicion para el filtro WHERE

  data := COALESCE(_opt->>'data', '[]');
  zona := COALESCE(_opt->>'zona', '20');
  sql := '
WITH
    coordenadas AS (
      SELECT 
       cast(r->>0 as int) as id,
       cast(r->>1 as float) as x,
       cast(r->>2 as float) as y,
       cast(' || zona || ' as int) as zonautm
      FROM
      (
       SELECT 
        value as r
       FROM
        json_array_elements(
        ''' || data || '''
        ) 
      ) t
      WHERE r->0 IS NOT NULL
    )
	SELECT 
      coor.id, coor.x, coor.y, coor.zonautm,
      cast(zp.ecorregion as text) as region
    from
    coordenadas coor
    left join 
    coberturas.zonas_productivas zp
    on (
      st_intersects(
          ST_Transform(
              ST_SetSRID(ST_Point(coor.x, coor.y),32700 + coor.zonautm),
              4326
          ),
          zp.the_geom
      )
    ) 
  ';
  RETURN QUERY EXECUTE sql;
	EXCEPTION
	WHEN others THEN
            RAISE EXCEPTION 'geoSICOB (sicob_zona_productiva), _opt:%, error:(%,%)', _opt::text, SQLERRM,SQLSTATE;	
END;
$function$
 