SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_zona_vida(_opt json)
 RETURNS TABLE(id integer, x double precision, y double precision, zonautm integer, codzona character varying, zonavida text)
 LANGUAGE plpgsql
AS $function$
DECLARE
sql text;
data text;
zona integer;
BEGIN
--PARAMETROS EN VARIABLE _opt
  --data, <-- Array de arrays con el formato [id,coorx,coordy] 
  --		que represanta una coordenada (sistema UTM) a ubicar en su coorrespondiente region.
  --zona  <-- (opcional) La zona UTM (19,20 o 21) de las coordenadas. Por defecto asume zona 20.
--Ej.: select * from sicob_zona_vida('{"data":[[1,280639,7998308],[2,280638,7998352]],"zona":"21"}')
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
      zv.cod as codzona,
      cast(zv.zon_vid as text) as zonavida
    from
    coordenadas coor
    left join 
    coberturas.zonas_vida zv
    on (
      st_intersects(
          ST_Transform(
              ST_SetSRID(ST_Point(coor.x, coor.y),32700 + coor.zonautm),
              4326
          ),
          zv.the_geom
      )
    ) 
  ';
  RETURN QUERY EXECUTE sql;
	EXCEPTION
	WHEN others THEN
            RAISE EXCEPTION 'geoSICOB (sicob_zona_vida), _opt:%, error:(%,%)', _opt::text, SQLERRM,SQLSTATE;	
END;
$function$
 