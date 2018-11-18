CREATE OR REPLACE FUNCTION public.sicob_snap_edge_debug(_edge geometry, _opt json)
 RETURNS geometry
 LANGUAGE plpgsql
 STRICT
AS $function$
DECLARE 
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
    FROM  processed.f20160708agfbdecf223329c_nsi_pred   --processed.f20160929daebgcf1933cf73_nsi 
WHERE (titulo IS NULL) and sicob_id = 22;

_opt = ('{"target":"coberturas.predios_proceso_geosicob_geo_201607", "tolerance":"' || (6::float/111000)::text || '"}')::json;
*/

/*
SELECT ST_ExteriorRing(
		(ST_Dump(the_geom)).geom) into _edge
    FROM  processed.f20160708agfbdecf223329c_nsi
WHERE sicob_id = 2;

_opt = ('{"target":"coberturas.predios_titulados", "tolerance":"' || (5.3::float/111000)::text || '"}')::json;
*/
	_tolerance := (_opt->>'tolerance')::real;
    _target := (_opt->>'target')::regclass;
    _returnpolygon := COALESCE((_opt->>'returnpolygon')::boolean,false);

--> Creando el buffer para extraer los bordes dentro de la zona de snapping
DROP TABLE IF EXISTS _buffer2snap;
CREATE TEMPORARY TABLE  _buffer2snap ON COMMIT DROP AS
SELECT st_union(ST_Buffer(_edge,  _tolerance * 1.2)) as the_geom;

 --Obteniendo la parte de b que se encuentra dentro de _buffer2snap
 _sql := '
  SELECT idpol_b, row_number() over() as id, (dmp).geom as the_geom
  from(
      SELECT idpol_b, st_dump(the_geom) as dmp  
      FROM (
          SELECT	b.' || COALESCE( sicob_feature_id(_target::text) ,'sicob_id') || ' as idpol_b,
          CASE 
              WHEN ST_CoveredBy(b.the_geom, a.the_geom) 
              THEN b.the_geom 
          ELSE 
			sicob_intersection(
    			a.the_geom, 
                b.the_geom,
                ''POLYGON''
             ) 
          END AS the_geom 
          FROM ' || _target::text || ' b,
          _buffer2snap a 
          where ST_Intersects(b.the_geom,a.the_geom)  AND NOT ST_Touches(b.the_geom, a.the_geom)    
      ) q
  ) p';

DROP TABLE IF EXISTS _b_into_2snap;   
RAISE DEBUG 'Running %', _sql;
EXECUTE 'CREATE TEMPORARY TABLE _b_into_2snap ON COMMIT DROP AS ' || _sql;

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

--> Creando el buffer de la zona de snapping
DROP TABLE IF EXISTS snapzone;
CREATE TEMPORARY TABLE  snapzone ON COMMIT DROP AS
SELECT st_union(ST_Buffer(_edge,  _tolerance)) as the_geom;


DROP TABLE IF EXISTS topo_face_dissolved;
CREATE TEMPORARY TABLE topo_face_dissolved ON COMMIT DROP AS
WITH
line_pol_b AS (
        SELECT idpol_b, (st_dump(the_geom)).geom as the_geom  
        FROM (
          SELECT idpol_b, st_intersection( st_exteriorring(  (ST_Dump(pol_b.the_geom)).geom   ), (SELECT the_geom from snapzone ) 
          ) as the_geom 
          FROM _pol_b pol_b
        ) r
),
 --Haciendo el dissolve de las lineas
line_b AS (
	SELECT row_number() over() as idpol_b, st_length(the_geom) as len, the_geom
	FROM (
		SELECT
		((
			st_dump(
				ST_UnaryUnion(st_collect((the_geom)))
			)
		).geom) as the_geom
		FROM 
			line_pol_b
	) t
),
--Obteniendo segmentos pequenos para revisar posibles colgantes
small_segments AS ( 
	SELECT *, sicob_anglediff(the_geom, shadow_geom) as angle , st_length(shadow_geom) as lensw
	FROM (
        SELECT *, sicob_shadowline(
            _edge, the_geom
        ) as shadow_geom
        FROM line_b
        WHERE
            len < 3 * _tolerance    
    )ss
),
topo_face_segments AS (
	SELECT the_geom
	FROM line_b
	WHERE idpol_b NOT IN (
		SELECT idpol_b FROM
		small_segments
		--WHERE angle < 5 AND lensw > (90*len/100)
		WHERE angle >= 5 OR lensw <= (90*len/100) --Eliminando ramificaciones
	)
),
topo_face_dissolved AS (
	SELECT 
    	row_number() over() as idpol_b,
        row_number() over() as segment,
		st_startpoint(the_geom) as pini, 
		st_endpoint(the_geom) as pfin,
        st_length(the_geom) as len,
        the_geom
	FROM (
      SELECT 
          (
			st_dump(
				ST_UnaryUnion(st_collect((the_geom)))
			)
          ).geom as the_geom
      FROM
        topo_face_segments
	)tf
)
SELECT * FROM topo_face_dissolved;


DROP TABLE IF EXISTS topo_face_ends_mask;
CREATE TEMPORARY TABLE topo_face_ends_mask ON COMMIT DROP AS
--> Recortando las puntas sobrantes **** 
WITH
topo_face_ends AS (
	SELECT 
    segment, 0 as pos, len, pini as pt
	FROM
    	topo_face_dissolved
	WHERE
    	len > 2* _tolerance
    UNION ALL
	SELECT 
    segment, st_npoints(the_geom) as pos,  len, pfin as pt
	FROM
    	topo_face_dissolved
	WHERE
    	len > 2* _tolerance    
),
topo_face_ends_info AS (
	SELECT *,
	--	st_astext(direction_line), st_astext(shadow_direction_line),
		sicob_anglediff(shadow_direction_line, direction_line) as angle
	FROM (
      SELECT *,
          ST_ShortestLine(
              pt,_edge
          )
          as direction_line,
          sicob_shadowline(
              (SELECT the_geom FROM topo_face_dissolved WHERE segment = t.segment), 
              ST_ShortestLine(
                  pt,_edge
              )
          ) as shadow_direction_line
      FROM 
        topo_face_ends t
	) u
),
topo_face_ends_mask AS (
	SELECT *,
		st_buffer(
                centermask,
                _tolerance
		) as mask
	FROM (
		SELECT *,
          CASE WHEN angle < 90 THEN
              CASE WHEN pos = 0 THEN
                  ST_LineInterpolatePoint(
                      (SELECT the_geom FROM topo_face_dissolved WHERE segment = u.segment),
                      _tolerance/len 
                  )
              ELSE
                  ST_LineInterpolatePoint(
                      (SELECT the_geom FROM topo_face_dissolved WHERE segment = u.segment),
                      1 - _tolerance/len 
                  )            
              END
          ELSE
              pt
          END as centermask    
        FROM   
        	topo_face_ends_info	u
    	WHERE angle IS NOT NULL
	) v
)
SELECT * FROM topo_face_ends_mask;

DROP TABLE IF EXISTS topo_face_cut_corner_fractions;
CREATE TEMPORARY TABLE topo_face_cut_corner_fractions ON COMMIT DROP AS
--> ? 
WITH
borde_in_mask AS (
SELECT *, 
	st_intersection( 
    	_edge, 
        mask
	) as borde 
	FROM topo_face_ends_mask
),
segments_borde_in_mask AS (
  SELECT
  	segment, pos, n AS nline,
  	ST_MakeLine(
  		ST_PointN((sl.g).geom,n),
  		ST_PointN((sl.g).geom,n+1)
  	) AS the_geom
  FROM
  	(SELECT segment, pos, ST_Dump(borde) AS g
  	FROM borde_in_mask) AS sl
  	CROSS JOIN
  	generate_series(1,10000) AS n
  	WHERE n < ST_NPoints((sl.g).geom)
  	ORDER by segment, pos, nline
),
segments_borde_in_mask_info AS (
	SELECT *,
    	(SELECT centermask FROM borde_in_mask 
        WHERE segment = sbm.segment AND pos = sbm.pos) as centermask
	FROM segments_borde_in_mask sbm
),
corner_cut_lines AS (
	SELECT *,
		sicob_scale(the_geom,_tolerance / st_length(the_geom) * 3, 
		_tolerance / st_length(the_geom) * 3) AS cut_line
	FROM segments_borde_in_mask_info sbmi
	WHERE
		ST_NPoints( -->Eliminando la lineas que no son parte de una esquina
			st_intersection(
            	st_exteriorring(
                	st_buffer(centermask,_tolerance-(0.05::float/111000))
                ), 
                the_geom
            )
		)<2
),
corner_cut_points_info AS (
        SELECT *, --Agregando la distancia al punto centermask y su posicion(fraccion) en topo_face
            st_distance(cut_point,centermask) as dist_centermask
        FROM (
          SELECT *, --Obteniendo el punto de interseccion entre cut_line y topo_face
              st_intersection(
                  cut_line,
				(SELECT the_geom FROM topo_face_dissolved WHERE segment = ccl.segment)
              ) as cut_point
          FROM corner_cut_lines ccl
          WHERE 
              st_crosses(
                  cut_line, 
				(SELECT the_geom FROM topo_face_dissolved WHERE segment = ccl.segment)
              )
		)pinter
        WHERE 
        	geometrytype(cut_point) = 'POINT'
),
corner_cut_points AS (
	SELECT * --Seleccionando el mas cercano al punto centermask (en caso de que exitan mas de 1)
	FROM corner_cut_points_info ccpi
	WHERE
		dist_centermask = (SELECT min(dist_centermask) 
			FROM corner_cut_points_info
			WHERE 
			segment = ccpi.segment AND pos = ccpi.pos
		)
),
topo_face_cut_corner_fractions AS (
	SELECT 
          ccp.segment,
          ccp.pos,
          st_linelocatepoint(
              (SELECT the_geom FROM topo_face_dissolved WHERE segment = ccp.segment), 
              cut_point
          ) AS cp_fraction
	FROM
		corner_cut_points ccp
)
SELECT * FROM topo_face_cut_corner_fractions;

DROP TABLE IF EXISTS topo_face;
CREATE TEMPORARY TABLE topo_face ON COMMIT DROP AS
--> ? 
WITH
topo_face_cut_corner_info AS ( --Calculando las fracciones para cortar las lineas topo_face
  SELECT DISTINCT(segment) segment,
      COALESCE(
          (SELECT cp_fraction 
              FROM topo_face_cut_corner_fractions 
              WHERE segment = tfc.segment AND pos=0
              LIMIT 1
          ),
          0
      ) as startfraction,
      COALESCE(
          (SELECT cp_fraction 
              FROM topo_face_cut_corner_fractions 
              WHERE segment = tfc.segment AND pos>0
              LIMIT 1
          ),
          1
      ) as endfraction
  FROM topo_face_cut_corner_fractions tfc
),
topo_face_cut_corner AS (
	SELECT segment,
    st_linesubstring(
    	(SELECT the_geom FROM topo_face_dissolved WHERE segment = tfci.segment),
        startfraction,
        endfraction
    ) as the_geom
    FROM 
    topo_face_cut_corner_info tfci
),

topo_face_cut_ends AS (
	SELECT segment, the_geom 
    FROM topo_face_dissolved 
    WHERE segment NOT IN (
    	SELECT segment FROM topo_face_cut_corner
    )
    UNION ALL
	SELECT * FROM topo_face_cut_corner
)
SELECT * FROM topo_face_cut_ends;

----------------------------------------

DROP TABLE IF EXISTS _shadow;    
 CREATE TEMPORARY TABLE _shadow ON COMMIT DROP AS   
 WITH   
_shadow1 AS ( 
	SELECT segment as segmentb, the_geom, sicob_shadowline(
		_edge, the_geom
	) as shadow_geom
	FROM topo_face
)
	SELECT segmentb, the_geom, shadow_geom, sicob_anglediff(the_geom, shadow_geom) as angle,
    st_length(the_geom) as lengthb FROM _shadow1;

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
	SELECT segmentb as id_segment, 0 as pospto, st_startpoint(the_geom) as pto FROM _shadowa
    UNION
    SELECT segmentb as id_segment, ST_NPoints(the_geom)-1 as pospto, st_endpoint(the_geom) as pto 
    FROM _shadowa
),
_ends_pts_dist AS (     
  SELECT t1.id_segment AS id_segmenta, t1.pospto as posptoa, t2.id_segment AS id_segmentb, t2.pospto as posptob, ST_Distance(t1.pto, t2.pto) as dist
  FROM _ends_pts_clipa t1, _ends_pts_segmentb t2 
  WHERE ST_DWithin(t1.pto, t2.pto, 3 * _tolerance) 
  ORDER BY t1.id_segment, ST_Distance(t1.pto, t2.pto) ASC
),
snapinf1 AS (
SELECT 
  t1.id_segmenta,
  t1.posptoa,
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
  sicob_setpoint(c.the_geom, i.posptoa,(SELECT t.pto FROM _ends_pts_segmentb t WHERE t.id_segment = i.id_segmentb AND t.pospto = i.posptob), (SELECT the_geom FROM topo_face WHERE segment=i.id_segmentb limit 1)),
  i2.posptoa,
  (SELECT t.pto FROM _ends_pts_segmentb t WHERE t.id_segment = i2.id_segmentb AND t.pospto = i2.posptob),
  (SELECT the_geom FROM topo_face WHERE segment=i.id_segmentb limit 1)
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
    sicob_setpoint(c.the_geom, i.posptoa, (SELECT t.pto FROM _ends_pts_segmentb t WHERE t.id_segment = i.id_segmentb AND t.pospto = i.posptob), (SELECT the_geom FROM topo_face WHERE segment=i.id_segmentb limit 1) ) AS the_geom
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
--SELECT ST_UnaryUnion(st_collect(la.the_geom,lb.the_geom)) into _newedge 
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