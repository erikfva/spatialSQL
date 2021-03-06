SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_dblink_connect(conn text)
 RETURNS character varying
 LANGUAGE plpgsql
AS $function$
DECLARE
    db text;
    username text;
    connectstring text;
    sql text;
BEGIN    
    SELECT current_database(),CURRENT_USER INTO db,username;
	connectstring := QUOTE_LITERAL('password=Abt2018* dbname=' || db || ' user=' || username);

	IF COALESCE(conn,'') = '' THEN
    	SELECT array_to_string(ARRAY(SELECT chr((97 + round(random() * 25)) :: integer) 
		FROM generate_series(1,15)), '') into conn;
    END IF;

	sql := 'SELECT dblink_connect(' || QUOTE_LITERAL(conn) || ',' || connectstring || ');';
	RAISE DEBUG 'Running %', sql;
	EXECUTE sql;
    RETURN conn;
EXCEPTION WHEN others THEN
    RAISE EXCEPTION 'sicob_dblink_connect: %, %', SQLERRM, SQLSTATE;
END;
$function$
 