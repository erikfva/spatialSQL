SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_execute(_query text, _opt json)
 RETURNS json
 LANGUAGE plpgsql
AS $function$
DECLARE
    _key text;
    _table_to_chunk text;
    _filter text;
    _txt_chunk_condition text;
    _txt_chunk_id text;
    _partitions int;
    _workers int;
    
    listSQL text[];
 
    sql text;
    db text;
    username text;
    connectstring text;
    conn text;
    r record;
    condition text;
    dispatch_result integer;
    dispatch_error text;
    n integer;
    num_done integer;
    status integer;
    processes jsonb[];
    result jsonb;
    results jsonb = '[]';
BEGIN

	--_query := 'SELECT sicob_obtener_predio(''{"lyr_in":"processed.f20170718fagebdcf580ac83_nsi","condition":"<chunk_condition>","subfix":"_p<chunk_id>","tolerance":"5.3"}'')';   
   -- _opt := '{"table_to_chunk":"processed.f20170718fagebdcf580ac83_nsi", "partitions":"20","workers":10}'::json;
	_workers := COALESCE( (_opt->>'workers')::int, 1);
SELECT sicob_paralellsql(
  sicob_splitSQL(_query::text,_opt::json)
,('{"max_connections":' || _workers || '}')::json) INTO results;
   RETURN results;

EXCEPTION WHEN others THEN
    RAISE EXCEPTION 'sicob_execute: _query -> %   %', SQLERRM, SQLSTATE;
	
END;
$function$
 