SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_to_geojson(_opt json)
 RETURNS json
 LANGUAGE plpgsql
AS $function$
DECLARE
  my_result json;
  _fix_dblquote boolean; 
  sql text;
  _maxdecimaldigits INTEGER;
  columns text;
  feature_id text;
BEGIN


--_opt := '{"lyr_in":"temp.f20190603ebgadfc9fddf40d_fixt_ppred", "fix_dblquote":true}'::json;	
--PARAMETROS EN VARIABLE _opt
  --lyr_in, <-- capa PostGIS a convertir 
  --condition TEXT  <-- (opcional) condicion para el filtro WHERE
  --fix_dblquote <-- Cambia " por Â¨ dentro del texto.
  --maxdecimaldigits <-- Cantidad de digitos decimales para las coordenadas.
  
_fix_dblquote := COALESCE((_opt->>'fix_dblquote')::boolean,FALSE);
_maxdecimaldigits := COALESCE((_opt->>'maxdecimaldigits')::integer,8);
columns := public.sicob_no_geo_column(_opt->>'lyr_in','{}','t.');
feature_id := sicob_feature_id((_opt->>'lyr_in')::regclass);
IF feature_id IS NULL THEN
	feature_id := 'sicob_id';
END IF;
IF feature_id <> 'sicob_id' THEN
	columns := columns || ',t.' || feature_id || ' as sicob_id';
END IF;
sql := '
SELECT row_to_json(fc)
     FROM ( 
       SELECT ''FeatureCollection'' AS type, array_to_json(array_agg(f)) AS features
       FROM ( 
         SELECT 
           ''Feature'' AS type,
           public.ST_AsGeoJSON(the_geom,' || _maxdecimaldigits::text || ')::json AS geometry,
           regexp_replace(
              regexp_replace(
                  row_to_json(
                    (SELECT p 
                        FROM (
                            SELECT -- Modify followinf cols from table t if needed:
                            ' || columns || '
                        ) AS p
                    )
                  )::text , 
                  ''\\r|\\n'', '''', ''g''
              ),
              ''\\"'', ''¨'', ''g''
           )::json                   
           AS properties           
         FROM ' || (_opt->>'lyr_in')::text || ' AS t -- <- Set your table here
         WHERE TRUE ' || COALESCE( 'AND (' ||  (_opt->>'condition')::text || ')', '') || ' -- <- Set some constraint if needed
         ORDER BY ' || feature_id || ' 
       ) AS f
     ) AS fc';
   RAISE DEBUG 'Running %', sql;
    
    EXECUTE  sql INTO my_result;
     RETURN my_result;
EXCEPTION
WHEN others THEN
            RAISE EXCEPTION 'geoSICOB (sicob_to_geojson) % , % , _opt: % | sql: % ', SQLERRM, SQLSTATE, _opt, sql;
END;
$function$
 