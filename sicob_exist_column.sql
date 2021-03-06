SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_exist_column(_tbl regclass, _col text)
 RETURNS boolean
 LANGUAGE sql
 STABLE
AS $function$
SELECT EXISTS (
   SELECT 1
   FROM   pg_catalog.pg_attribute
   WHERE  attrelid = $1
   AND    attname  = $2
   AND    NOT attisdropped
   AND    attnum   > 0
   )
$function$
 