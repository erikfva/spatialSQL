SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_store_in_repository(_opt json)
 RETURNS json
 LANGUAGE plpgsql
AS $function$
DECLARE 
 _out json := '{}';
 -------------------------------

BEGIN

---------------------------
--PARAMETROS DE ENTRADA
---------------------------
--> lyr : capa de poligonos que se almacenara en el repositorio.
--> condition (opcional): Filtro para los datos de "lyr". Si no se especifica, se toman todos los registros.
--> repository : nombre de la tabla 'repositorio' donde se almacenaran los registros.
--> add (opcional true/false): Si exiten registros en repositorio con el valor de layerid igual al lyr,
-->		no son eliminados.

---------------------------
--VALORES DEVUELTOS
---------------------------
--> row_cnt : Cantidad de registros insertados.

IF (COALESCE((_opt->>'add')::boolean,FALSE)) = FALSE THEN
	EXECUTE 'DELETE FROM ' || (_opt->>'repository')::text ||
    ' WHERE layerid = ''' ||
    (sicob_split_table_name((_opt->>'lyr')::text)).table_name || ''' OR ' ||
    'layerid = ''' || ((_opt->>'lyr')::text) || '''';
END IF;

SELECT sicob_executesql('
INSERT INTO ' || (_opt->>'repository')::text || '(layerid, data, the_geom)
SELECT ''' || (sicob_split_table_name((_opt->>'lyr')::text)).table_name || ''' as layerid, data, the_geom from (
  select 
  sicob_id,
    (
      select row_to_json(d)
      from (
        select ' || 
        sicob_no_geo_column(
          '' || (_opt->>'lyr')::text || '',
          '{gid,sicob_utm,sicob_sup,topogeom}',
          'd.'
        ) || '
        from ' || (_opt->>'lyr')::text || ' d
        where d.sicob_id=s.sicob_id
      ) d
    ) as data,
    the_geom
  from ' || (_opt->>'lyr')::text || ' s
) t','{}'::json) INTO _out;

  
RETURN _out;

/** FIN!!! **/


EXCEPTION
WHEN others THEN
            RAISE EXCEPTION 'geoSICOB (sicob_store_in_repository) % , % , _opt: % ', SQLERRM, SQLSTATE, _opt;	
END;
$function$
 