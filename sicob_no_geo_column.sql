SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_no_geo_column(reloid text, exclude_cols text[], _prefix text)
 RETURNS text
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
AS $function$
--DEVUELVE LOS CAMPOS NO GEOMETRICOS DE UNA TABLA SEPARADOS POR COMA, EXCLUYENDO LOS CAMPOS INDICADOS EN 
-- 'exclude_cols' Y ANTECEDIENDO A CADA NOMBRE DE CAMPO EL VALOR DE 'prefix'
--Call: SELECT * FROM sicob_no_geo_column('uploads.tt','{sicob_id}','poly.');
DECLARE 
  cols text;
  __prefix text;
   tbl_name text; sch_name text;
BEGIN

SELECT * FROM public.sicob_split_table_name(reloid) INTO sch_name, tbl_name;

  IF _prefix IS NOT NULL AND _prefix <> '' THEN
    __prefix := CASE WHEN _prefix = ' ' THEN '' ELSE _prefix END; --> Si no se desea anteponer prefijos entonces enviar como 1 espacio vacio " ".
  ELSE
    __prefix := reloid::text || '.';
  END IF;

  IF sch_name <> '' THEN
  
  	SELECT string_agg(__prefix || column_name, ',') as columnas into cols FROM (
		SELECT column_name
		FROM information_schema.columns c
		WHERE table_schema=sch_name AND table_name=tbl_name
    	AND c.column_name not in (
			SELECT gc.f_geometry_column FROM public.geometry_columns gc, pg_class c, pg_namespace n 
    		WHERE c.oid = reloid::regclass  AND n.oid = c.relnamespace
     		AND gc.f_table_schema = n.nspname AND gc.f_table_name = c.relname
        )
        AND c.column_name <> ALL ($2)
      	ORDER BY ordinal_position
	) t;

  
  	IF cols IS NULL THEN
          SELECT string_agg(__prefix || column_name, ',') as columnas into cols FROM (    
          select attname AS column_name from pg_attribute where attrelid = reloid::regclass
          AND attname not in ('the_geom','the_geom_webmercator', 'ctid', 'cmax', 'xmax', 'cmin', 'xmin', 'tableoid', 'oid')
              AND attname <> ALL ($2)
      	) t;
  	END IF;
  
      IF cols IS NULL THEN
          cols := '[NO ENCONTRADO]';
      END IF;
      RETURN cols;
  END IF;

	--> intentando como tabla temporal
    /*
    SELECT string_agg(__prefix || column_name, ',') as columnas into cols FROM (
        SELECT column_name
        FROM information_schema.columns c
        WHERE table_schema like 'pg_temp%' AND table_name=tbl_name
        AND c.column_name not in (
                SELECT gc.f_geometry_column FROM public.geometry_columns gc, pg_class c, pg_namespace n 
                WHERE c.oid = reloid::regclass  AND n.oid = c.relnamespace
                AND gc.f_table_schema = n.nspname AND gc.f_table_name = c.relname
            )
            AND c.column_name <> ALL ($2)
          ORDER BY ordinal_position
    ) t;
    */
    SELECT string_agg(__prefix || column_name, ',') as columnas into cols FROM (    
    	select attname AS column_name from pg_attribute where attrelid = tbl_name::regclass
        AND attname not in ('the_geom','the_geom_webmercator', 'ctid', 'cmax', 'xmax', 'cmin', 'xmin', 'tableoid', 'oid')
            AND attname <> ALL ($2)
    ) t;
        
    IF cols IS NULL THEN
    	cols := '[NO ENCONTRADO]';
    END IF;

RETURN cols;
EXCEPTION
WHEN others THEN
	RAISE EXCEPTION 'geoSICOB (sicob_no_geo_column): % (%)', SQLERRM, SQLSTATE;
END;
$function$
 