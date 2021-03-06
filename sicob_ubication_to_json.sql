SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_ubication_to_json(reloid text)
 RETURNS json
 LANGUAGE sql
AS $function$
WITH
t as (
SELECT * FROM sicob_ubication(reloid)
)
SELECT row_to_json(r) as ubication
FROM 
(
	SELECT array_agg(t.sicob_id) AS sicob_id,
           array_agg(t.nom_mun) AS nom_mun,
           array_agg(t.nom_prov) AS nom_prov,
           array_agg(t.nom_dep) AS nom_dep
      FROM  t
) r;
$function$
 