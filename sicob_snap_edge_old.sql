SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_snap_edge_old(_edge geometry, _opt json)
 RETURNS geometry
 LANGUAGE plpgsql
 STRICT
AS $function$
DECLARE 
    _buffer2snap geometry;
    _sql text;
    _newedge geometry;
    row_cnt integer;
    _tolerance double precision;
    _target regclass;
    _returnpolygon boolean;
BEGIN

/*
SELECT ST_ExteriorRing(
		(ST_Dump(the_geom)).geom) into _edge
    FROM  processed.f20160708agfbdecf223329c_nsi_pred  --processed.f20160929daebgcf1933cf73_nsi 
WHERE (titulo IS NULL) and sicob_id = 36;

_tolerance := 6::float/111000;

_target := 'coberturas.predios_proceso_geosicob_geo_201607';

_opt = ('{"target":"coberturas.predios_proceso_geosicob_geo_201607", "tolerance":"' || (6::float/111000)::text || '"}')::json;

_edge = ST_GeomFromText('LINESTRING(-65.9571809346542 -14.8449821172025,-65.957168541 -14.844994331,-65.9322121818794 -14.8534219022309,-65.9364639406954 -14.8657132723635,-65.939873996 -14.865236045,-65.944718211 -14.879963001,-65.94477589 -14.880138335,-65.944709756 -14.880145796,-65.9441799209999 -14.880038009,-65.9437111989999 -14.879834729,-65.943149935 -14.8795368909999,-65.94268565 -14.879266967,-65.942123776 -14.8790362779999,-65.941723114 -14.8787604679999,-65.941041409 -14.8786748699999,-65.9409655200684 -14.8787268464274,-65.9488446111732 -14.9015044378352,-65.957936698 -14.9003251689999,-65.961116976 -14.9010172979999,-65.965330965 -14.892555597,-65.9653747689999 -14.893864793,-65.9654383399999 -14.894504132,-65.9654313199999 -14.8947924299999,-65.965364467 -14.895056171,-65.9652817639999 -14.895382394,-65.965197136 -14.895846875,-65.965149281 -14.896562837,-65.965103498 -14.897330473,-65.9650874559999 -14.898208956,-65.9650728839999 -14.8991649939999,-65.965034277 -14.9003405459999,-65.9649017409999 -14.9017197749999,-65.964168431 -14.9036005579999,-65.963005466 -14.9044250339999,-65.960407061 -14.90546502,-65.957335105 -14.9066855039999,-65.9523706909999 -14.908150829,-65.9512706888308 -14.9085179632374,-65.9515667587308 -14.9093738689656,-65.9526563649999 -14.9090102,-65.9576462489999 -14.9075373479999,-65.96075939 -14.9063005039999,-65.9634622259999 -14.905218702,-65.9649387489999 -14.9041719429999,-65.9658145889999 -14.9019255539999,-65.9659614729999 -14.900397015,-65.966001225 -14.899186094,-65.9660158899999 -14.898223661,-65.966031583 -14.8973646539999,-65.966076093 -14.8966182569999,-65.966120399 -14.895955252,-65.9661906869999 -14.895569442,-65.9662660239999 -14.8952722949999,-65.966357089 -14.8949071769999,-65.9663678429999 -14.8944658139999,-65.966302191 -14.893805561,-65.966264156 -14.89267668,-65.966351545 -14.892407245,-65.973493897207 -14.8964327737694,-65.9790346709007 -14.8801712681255,-65.9571809346542 -14.8449821172025)', 4326);
*/
	_tolerance := (_opt->>'tolerance')::real;
    _target := (_opt->>'target')::regclass;
    _returnpolygon := COALESCE((_opt->>'returnpolygon')::boolean,false);
	_buffer2snap := st_union(ST_Buffer(_edge,  _tolerance * 2, 'join=bevel'));
 
_sql := 'DROP TABLE IF EXISTS _b_into_2snap';
RAISE DEBUG 'Running %', _sql;
EXECUTE _sql;
 --Obteniendo la parte de b que se encuentra dentro de _buffer2snap 
_sql := '
CREATE TEMPORARY TABLE _b_into_2snap ON COMMIT DROP AS 
  SELECT idpol_b, row_number() over() as id, (dmp).geom as the_geom
  from(
      SELECT idpol_b, st_dump(the_geom) as dmp  
      FROM (
          SELECT	b.' || COALESCE( sicob_feature_id(_target::text) ,'sicob_id') || ' as idpol_b,
          CASE 
              WHEN ST_CoveredBy(b.the_geom, $1) 
              THEN b.the_geom 
          ELSE 
              ST_Multi(
                  ST_Intersection($1,b.the_geom)
              ) 
          END AS the_geom 
          FROM ' || _target::text || ' b 
          WHERE ST_Intersects(b.the_geom,$1)  AND NOT ST_Touches(b.the_geom, $1)    
      ) q
  ) p
  ';
 EXECUTE _sql USING _buffer2snap; 

RAISE DEBUG 'Running %', _sql;

GET DIAGNOSTICS row_cnt = ROW_COUNT;
IF row_cnt < 1 THEN --si no tiene registros, no existe sobreposicion
	IF _returnpolygon = true THEN
		RETURN st_makepolygon( _edge);
    ELSE
		RETURN _edge;
    END IF;
END IF;

DROP TABLE IF EXISTS _pol_b;

IF row_cnt = 1 THEN --si tiene 1 solo registros, no se necesita normalizar los bordes
	CREATE TEMPORARY TABLE _pol_b ON COMMIT DROP AS
    SELECT * FROM _b_into_2snap; 
ELSE
	--NORMALIZANDO los bordes
    CREATE TEMPORARY TABLE _pol_b ON COMMIT DROP AS
    WITH
    pol_b_not_overlayed AS (  
        SELECT idpol_b, id, the_geom FROM _b_into_2snap
        WHERE id not IN ( 
        --filtrando los polignos que se sobreponenen dentro de la zona de snapping
            SELECT 
            CASE WHEN st_area(b1.the_geom) < st_area(b2.the_geom) THEN
                b1.id
            ELSE
                b2.id
            END as idexclude
            FROM
                _b_into_2snap b1,
                _b_into_2snap b2
            WHERE 
                b1.id <> b2.id and
                b1.id < b2.id and
                ST_Intersects(b1.the_geom,b2.the_geom) 
                AND NOT ST_Touches(b1.the_geom, b2.the_geom)   
        )
    )
	SELECT *
	FROM pol_b_not_overlayed;  
END IF;

DROP TABLE IF EXISTS _line_pol_b;
CREATE TEMPORARY TABLE _line_pol_b ON COMMIT DROP AS
WITH
_snapzone AS (    
	SELECT st_union(ST_Buffer(_edge,  _tolerance, 'endcap=flat join=bevel' )) as the_geom 
),
line_b AS (
 --Obteniendo las lineas del poligono pol_b dentro de la zona de snapping snapzone
	SELECT (dp).geom as the_geom
    FROM(
        SELECT idpol_b, st_dump(the_geom) as dp  
        FROM (
          SELECT idpol_b, st_intersection(sicob_polygon_to_line((ST_Dump(pol_b.the_geom)).geom), (SELECT st_union(the_geom) from _snapzone ) 
          ) as the_geom 
          FROM _pol_b pol_b
        ) r
    ) q
),
line_b_merged AS (
 SELECT st_linemerge( ST_Collect(the_geom) ) as the_geom from line_b
)
SELECT 
 row_number() over() as  idpol_b, 
 row_number() over() as segment,
 st_startpoint((dp).geom) as pini, 
 st_endpoint((dp).geom) as pfin,
 (dp).geom as the_geom
FROM 
	(SELECT 
		st_dump(the_geom) as dp
	FROM 
		line_b_merged
    ) t;

DROP TABLE IF EXISTS _shadow;    
 CREATE TEMPORARY TABLE _shadow ON COMMIT DROP AS   
 WITH   
_shadow1 AS ( 
	SELECT idpol_b, segment as segmentb, the_geom, sicob_shadowline(
		_edge, the_geom
	) as shadow_geom
	FROM _line_pol_b
)
	SELECT idpol_b, segmentb, the_geom, shadow_geom, sicob_anglediff(the_geom, shadow_geom) as angle,
    st_length(the_geom) as lengthb, |/5 / 2 * _tolerance as max_size FROM _shadow1;

DROP TABLE IF EXISTS _shadowa;
CREATE TEMPORARY TABLE _shadowa ON COMMIT DROP AS
	SELECT * FROM _shadow
	WHERE 
    	(angle < 15 OR angle >= 345) OR (angle >= 165 AND angle <= 195 ) --solo aquella lineas que tengan +-15 grados ente ellas
		OR 
		(st_npoints(the_geom) > 2 AND (angle < 40 OR angle > 320 OR (angle > 150 AND angle < 210)   ));

	GET DIAGNOSTICS row_cnt = ROW_COUNT;
	IF row_cnt < 1 THEN 
    	--si no tiene registros, no existen segmentos de b que
        --formen un angulo con el borde de tal forma 
        --que se considere suficiente para snapping.
		IF _returnpolygon = true THEN
			RETURN st_makepolygon( _edge);
    	ELSE
			RETURN _edge;
    	END IF;
	END IF;				
            
WITH
_maskshadowa AS (  
	SELECT  segmentb,
    st_buffer(
    shadow_geom
    ,0.0000001 ) as the_geom
    FROM _shadowa
),
_clipa AS (    
	SELECT row_number() over() as segmenta,(dmp).geom as the_geom, 
		st_startpoint((dmp).geom) as pini, st_endpoint((dmp).geom) as pfin
	FROM(
 		SELECT st_dump( st_linemerge(st_difference( _edge , m.the_geom )) ) as dmp 
 		FROM
 		(select st_union(the_geom) as the_geom FROM  _maskshadowa) m
 	)t
),
_ends_pts_clipa AS (
	SELECT segmenta as id_segment, 0 as pospto, pini as pto FROM _clipa
    UNION
    SELECT segmenta as id_segment, ST_NPoints(the_geom)-1 as pospto, pfin as pto FROM _clipa
),
_ends_pts_segmentb AS (  
	SELECT idpol_b, segmentb as id_segment, 0 as pospto, st_startpoint(the_geom) as pto FROM _shadowa
    UNION
    SELECT idpol_b, segmentb as id_segment, ST_NPoints(the_geom)-1 as pospto, st_endpoint(the_geom) as pto 
    FROM _shadowa
),
_ends_pts_dist AS (     
  SELECT t1.id_segment AS id_segmenta, t1.pospto as posptoa,t2.idpol_b, t2.id_segment AS id_segmentb, t2.pospto as posptob, ST_Distance(t1.pto, t2.pto) as dist
  FROM _ends_pts_clipa t1, _ends_pts_segmentb t2 
  WHERE ST_DWithin(t1.pto, t2.pto, 3 * _tolerance) 
  ORDER BY t1.id_segment, ST_Distance(t1.pto, t2.pto) ASC
),
snapinf1 AS (
SELECT 
  t1.id_segmenta,
  t1.posptoa,
  t1.idpol_b,
  t1.id_segmentb,
  t1.posptob,
  t1.dist
FROM
  _ends_pts_dist t1
WHERE
	t1.posptoa = 0 AND
  t1.dist = (SELECT t2.dist FROM _ends_pts_dist t2 WHERE t2.id_segmenta = t1.id_segmenta 		
  AND t2.posptoa = t1.posptoa ORDER BY t2.dist LIMIT 1) 
),
snapinf2 AS (
SELECT 
  t1.id_segmenta,
  t1.posptoa,
  t1.idpol_b,
  t1.id_segmentb,
  t1.posptob,
  t1.dist
FROM
  _ends_pts_dist t1
WHERE
	t1.posptoa > 0
  AND
  t1.posptob NOT IN (SELECT sn1.posptob FROM snapinf1 sn1 WHERE  sn1.id_segmentb = t1.id_segmentb AND sn1.posptob = t1.posptob)
),
_snapinf AS (   
	SELECT * FROM snapinf1
	UNION
	SELECT * FROM snapinf2
),
_snapclipa1 AS (
SELECT 
  i.id_segmenta,
  sicob_setpoint(
  sicob_setpoint(c.the_geom, i.posptoa,(SELECT t.pto FROM _ends_pts_segmentb t WHERE      t.idpol_b = i.idpol_b AND t.id_segment = i.id_segmentb AND t.pospto = i.posptob), (SELECT the_geom FROM _line_pol_b WHERE idpol_b=i.idpol_b AND segment=i.id_segmentb limit 1)),
  i2.posptoa,
  (SELECT t.pto FROM _ends_pts_segmentb t WHERE t.idpol_b=i2.idpol_b AND t.id_segment = i2.id_segmentb AND t.pospto = i2.posptob),
  (SELECT the_geom FROM _line_pol_b WHERE idpol_b=i.idpol_b AND segment=i.id_segmentb limit 1)
  )  AS the_geom
FROM
  _snapinf i
  INNER JOIN _snapinf i2 ON (i.id_segmenta = i2.id_segmenta)
  INNER JOIN _clipa c ON (i.id_segmenta = c.segmenta)
WHERE
  i.posptoa = 0 AND 
  i2.posptoa > 0
),
_snapclipa2 AS (  
  SELECT 
    c.segmenta,
    sicob_setpoint(c.the_geom, i.posptoa, (SELECT t.pto FROM _ends_pts_segmentb t WHERE t.idpol_b=i.idpol_b AND t.id_segment = i.id_segmentb AND t.pospto = i.posptob), (SELECT the_geom FROM _line_pol_b WHERE idpol_b=i.idpol_b AND segment=i.id_segmentb limit 1) ) AS the_geom
  FROM
    _clipa c
    INNER JOIN _snapinf i ON (c.segmenta = i.id_segmenta)
  WHERE
    i.id_segmenta NOT IN (SELECT id_segmenta FROM _snapclipa1)
),
_snapclipa AS (   
	SELECT st_union(snp.the_geom) as the_geom 
	FROM
	(SELECT the_geom as the_geom FROM _snapclipa1
     UNION ALL
	SELECT the_geom as the_geom FROM _snapclipa2) snp
)   
 SELECT st_linemerge( st_union(la.the_geom,lb.the_geom) ) into _newedge
 FROM (SELECT the_geom FROM _snapclipa) la,
 (select st_union(the_geom) as the_geom FROM _shadowa) lb;
 	
	if st_geometrytype(_newedge) not in ('ST_LineString') then
    	_newedge := ST_BuildArea(_newedge);   
        IF _returnpolygon = false THEN
        	_newedge := st_exteriorring( _newedge);
        END IF; 
        IF ST_IsClosed(_newedge) THEN
        	RETURN _newedge; 
        END IF;
    /*
    	_newedge := st_exteriorring( ST_BuildArea( st_union(_newedge, 
        COALESCE(sicob_doors(_newedge, _tolerance), 'LINESTRING EMPTY')  
        )   ));
        */
    END IF;
    
    --Fix error de borde abierto: ERROR: lwpoly_from_lwlines: shell must be closed
    IF ST_IsClosed(_newedge) = false THEN
    	_newedge := sicob_forceclosed(_newedge);
    END IF;
    
    IF _returnpolygon = true THEN
       _newedge := st_makepolygon( _newedge);
    END IF;     
    
	RETURN _newedge;     

EXCEPTION
WHEN others THEN
            RAISE EXCEPTION 'geoSICOB (sicob_snap_edge): % (%) | _edge: % | _target: %', SQLERRM, SQLSTATE, st_astext(_edge), _target::text;	
END;
$function$
 