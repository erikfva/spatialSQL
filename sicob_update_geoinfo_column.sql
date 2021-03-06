SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_update_geoinfo_column()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
        IF (TG_OP = 'INSERT') OR (TG_OP = 'UPDATE' AND NOT (NEW.the_geom ~= OLD.the_geom) ) THEN
		NEW.the_geom_webmercator := SICOB_transformtowebmercator(NEW.the_geom);
        NEW.sicob_utm := SICOB_utmzone_wgs84(NEW.the_geom);
		NEW.sicob_sup := round((ST_Area(ST_Transform(NEW.the_geom, NEW.sicob_utm::integer))/10000)::numeric,4); --superficie en hectareas
        
        END IF;
  RETURN NEW;
END;
$function$
;CREATE OR REPLACE FUNCTION public.sicob_update_geoinfo_column(reloid text)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE 
 sql TEXT;
BEGIN

sql := Format('UPDATE %s SET sicob_utm = SICOB_utmzone_wgs84(the_geom),the_geom_webmercator = SICOB_transformtowebmercator(the_geom), 
sicob_sup = round((ST_Area(ST_Transform(the_geom, SICOB_utmzone_wgs84(the_geom)))/10000)::numeric,5)', reloid::text);
        RAISE DEBUG 'sicob_update_geoinfo_column(reloid): %', sql;
        EXECUTE sql;
END;
$function$
 