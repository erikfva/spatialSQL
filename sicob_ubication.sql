SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_ubication(reloid text)
 RETURNS TABLE(sicob_id integer, nom_mun text, nom_prov text, nom_dep text)
 LANGUAGE plpgsql
AS $function$
BEGIN

RETURN QUERY 

SELECT * FROM sicob_ubication(reloid,'TRUE');


	EXCEPTION
	WHEN others THEN
            RAISE EXCEPTION 'geoSICOB (sicob_ubication); cobertura:%, error:(%,%)', reloid::text, SQLERRM,SQLSTATE;	

END;
$function$
;CREATE OR REPLACE FUNCTION public.sicob_ubication(reloid text, condition text)
 RETURNS TABLE(sicob_id integer, nom_mun text, nom_prov text, nom_dep text)
 LANGUAGE plpgsql
AS $function$
BEGIN

RETURN QUERY 

SELECT 
	t.sicob_id, 
	string_agg(t.nom_mun, ',') as nom_mun,
	string_agg(t.nom_prov, ',') as nom_prov,
	string_agg(t.nom_dep, ',') as nom_dep
FROM 
	(SELECT * FROM sicob_ungroup_ubication(reloid, condition)) t
group by t.sicob_id
order by t.sicob_id;


	EXCEPTION
	WHEN others THEN
            RAISE EXCEPTION 'geoSICOB (sicob_ubication); cobertura:%, error:(%,%)', reloid::text, SQLERRM,SQLSTATE;	

END;
$function$
 