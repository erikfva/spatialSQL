SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_exist_table(reloid text)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
--VERIFICA SI EXISTE UNA TABLA
DECLARE 
   tbl_name text; sch_name text;
BEGIN
	SELECT * FROM sicob_split_table_name(reloid) INTO sch_name, tbl_name;
	RETURN EXISTS(SELECT * FROM information_schema.tables WHERE table_schema = sch_name AND table_name = tbl_name);
EXCEPTION
	WHEN others THEN
		RAISE EXCEPTION 'geoSICOB (sicob_exist_table):%,  % (%)', reloid::text , SQLERRM, SQLSTATE;
END;
$function$
 