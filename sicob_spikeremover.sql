SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_spikeremover(geometry)
 RETURNS geometry
 LANGUAGE sql
 STRICT
AS $function$
WITH
target AS (
	SELECT $1 as the_geom
),
source AS (
	SELECT
      st_buffer(
            st_buffer(
                the_geom ,
                -0.000001,
                'quad_segs=3'
            ),
            0.000001,
            'quad_segs=3'
      ) as the_geom
	FROM 
    	target
),
edge AS (
    SELECT st_union( (ST_ExteriorRing(the_geom)) ) as the_edge
        FROM (
            SELECT (dp).path[1] as gid, (dp).geom as the_geom 
            FROM(
                SELECT st_dump(the_geom) as dp FROM source
                ) t        
        ) As u
    GROUP BY gid
),
boundaries AS (
    SELECT   st_union( st_boundary(the_geom))  as boundaries
        FROM target
),
_params AS (
SELECT 
    edge,
    (SELECT st_union(ST_Buffer(the_edge, tolerance )) FROM edge)  as buffer_snap,
    tolerance,
    TRUE as returnpolygon,
    boundaries
 FROM (
 	SELECT 
    (SELECT the_edge FROM edge) as edge,
    (SELECT boundaries FROM boundaries) as boundaries,
    0.000001 as tolerance
 ) params
),
line_b AS (
	SELECT 
		st_intersection(boundaries,buffer_snap) as the_geom
	FROM _params
),
segments_b AS (
  SELECT row_number() over() as gid, the_geom 
  FROM(
      SELECT (st_dump(the_geom)).geom as the_geom FROM (
          SELECT ST_LineMerge(the_geom) the_geom FROM line_b
      )u
  )t 
),
dangles AS (
SELECT 
a.gid, a.the_geom
FROM 
segments_b a 
WHERE
EXISTS (
	SELECT gid FROM segments_b b
    WHERE b.gid<>a.gid AND
    st_intersects(a.the_geom,b.the_geom) limit 1
)
AND (
NOT EXISTS (
	SELECT gid FROM segments_b b
    WHERE 
    b.gid<>a.gid AND
    st_intersects(st_startpoint(a.the_geom),b.the_geom)
)
OR
NOT EXISTS (
	SELECT gid FROM segments_b b
    WHERE 
    b.gid<>a.gid AND
    st_intersects(st_endpoint(a.the_geom),b.the_geom)
))
),
smallest_dangles AS (
	SELECT
    gid, the_geom
    FROM 
    dangles a 
    WHERE
    NOT EXISTS ( 
    	SELECT gid 
        FROM segments_b b 
        WHERE 
        	a.gid <>b.gid AND 
            st_intersects(a.the_geom,b.the_geom) AND 
            st_length(b.the_geom) < st_length(a.the_geom)
    )
),
topo_face1 AS (
	SELECT 
        gid as idpol_b,
        gid as segment,
		st_startpoint(the_geom) as pini, 
		st_endpoint(the_geom) as pfin,
        st_length(the_geom) as len,
        the_geom
    FROM 
    segments_b a
    WHERE
    NOT EXISTS(
    	SELECT gid FROM smallest_dangles b
        WHERE a.gid = b.gid
    )
),
--> Recortando las puntas sobrantes **** 
topo_face_ends AS (
	SELECT 
    segment, 0 as pos, len, pini as pt
	FROM
    	topo_face1
	WHERE
    	len > 2* (SELECT tolerance FROM _params)
    UNION ALL
	SELECT 
    segment, st_npoints(the_geom) as pos,  len, pfin as pt
	FROM
    	topo_face1
	WHERE
    	len > 2* (SELECT tolerance FROM _params)    
),
topo_face_ends_info AS (
	SELECT *,
	--	st_astext(direction_line), st_astext(shadow_direction_line),
		sicob_anglediff(shadow_direction_line, direction_line) as angle
	FROM (
      SELECT *,
          ST_ShortestLine(
              pt,(SELECT edge FROM _params)
          )
          as direction_line,
          sicob_shadowline(
              (SELECT the_geom FROM topo_face1 WHERE segment = t.segment), 
              ST_ShortestLine(
                  pt,(SELECT edge FROM _params)
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
                (SELECT tolerance FROM _params)
		) as mask
	FROM (
		SELECT *,
          CASE WHEN angle < 90 THEN
              CASE WHEN pos = 0 THEN
                  ST_LineInterpolatePoint(
                      (SELECT the_geom FROM topo_face1 WHERE segment = u.segment),
                      (SELECT tolerance FROM _params)/len 
                  )
              ELSE
                  ST_LineInterpolatePoint(
                      (SELECT the_geom FROM topo_face1 WHERE segment = u.segment),
                      1 - (SELECT tolerance FROM _params)/len 
                  )            
              END
          ELSE
              pt
          END as centermask    
        FROM   
        	topo_face_ends_info	u
    	WHERE angle IS NOT NULL
	) v
),
borde_in_mask AS (
SELECT *, 
	st_intersection( 
    	(SELECT edge FROM _params), 
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
		sicob_scale(the_geom,(SELECT tolerance FROM _params) / st_length(the_geom) * 3, 
		(SELECT tolerance FROM _params) / st_length(the_geom) * 3) AS cut_line
	FROM segments_borde_in_mask_info sbmi
	WHERE
		ST_NPoints( -->Eliminando la lineas que no son parte de una esquina
			st_intersection(
            	st_exteriorring(
                	st_buffer(centermask,(SELECT tolerance FROM _params)-(0.05::float/111000))
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
				(SELECT the_geom FROM topo_face1 WHERE segment = ccl.segment)
              ) as cut_point
          FROM corner_cut_lines ccl
          WHERE 
              st_crosses(
                  cut_line, 
				(SELECT the_geom FROM topo_face1 WHERE segment = ccl.segment)
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
              (SELECT the_geom FROM topo_face1 WHERE segment = ccp.segment), 
              cut_point
          ) AS cp_fraction
	FROM
		corner_cut_points ccp
),
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
    	(SELECT the_geom FROM topo_face1 WHERE segment = tfci.segment),
        CASE WHEN startfraction < endfraction THEN startfraction ELSE endfraction END,
        CASE WHEN startfraction < endfraction THEN endfraction ELSE startfraction END
    ) as the_geom
    FROM 
    topo_face_cut_corner_info tfci
),
topo_face_cut_ends AS (
	SELECT segment, the_geom 
    FROM topo_face1 
    WHERE segment NOT IN (
    	SELECT segment FROM topo_face_cut_corner
    )
    UNION ALL
	SELECT * FROM topo_face_cut_corner
),
topo_face AS (
	SELECT * FROM topo_face_cut_ends
),
_shadow1 AS ( 
	SELECT segment as segmentb, the_geom, sicob_shadowline(
		(SELECT edge FROM _params), the_geom
	) as shadow_geom
	FROM topo_face
),
_shadow AS (
	SELECT segmentb, the_geom, shadow_geom, sicob_anglediff(the_geom, shadow_geom) as angle,
    st_length(the_geom) as lengthb FROM _shadow1
    where st_length(shadow_geom) < 1.5 * st_length(the_geom)
),
_shadowa AS (
	SELECT * FROM _shadow
	WHERE 
    	(angle < 15 OR angle >= 345) OR (angle >= 165 AND angle <= 195 ) --solo aquella lineas que tengan +-15 grados ente ellas
		OR 
		(st_npoints(the_geom) > 2 AND (angle < 40 OR angle > 320 OR (angle > 150 AND angle < 210)   ))
),
_maskshadowa AS (  
	SELECT  segmentb,
    st_buffer(
    shadow_geom
    ,0.0000000001 ) as the_geom
    FROM _shadowa
),
_clipa AS (    
	SELECT row_number() over() as segmenta,(dmp).geom as the_geom, 
		st_startpoint((dmp).geom) as pini, st_endpoint((dmp).geom) as pfin
	FROM(
 		SELECT st_dump( st_linemerge(st_difference( (SELECT edge FROM _params limit 1) , m.the_geom )) ) as dmp 
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
  WHERE ST_DWithin(t1.pto, t2.pto, 3 * (SELECT tolerance FROM _params limit 1) ) 
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
),
_buildedge AS ( 
SELECT
	CASE WHEN (SELECT count(*) FROM _shadowa) > 0 THEN 
        (
            SELECT
                CASE WHEN ST_IsClosed(the_geom) THEN
                    the_geom
                ELSE
                	--CERRANDO EL BORDE  
                    st_linemerge(                  
                    ST_Collect(
                        the_geom,  
                        sicob_doors(the_geom, 0.00009)    
                    ))
                END
            FROM
            (
              SELECT st_linemerge(st_collect(the_geom)) as the_geom 
              FROM (
                  SELECT  (st_dump(the_geom)).geom as the_geom FROM _shadowa
                  UNION ALL
                  SELECT  (st_dump(the_geom)).geom as the_geom FROM _snapclipa
              ) t
            ) u
        )
	ELSE
		(SELECT edge FROM _params)
    END  as the_geom 
),
_builddoors AS (
	SELECT sicob_doors(the_geom, 0.00009) as the_geom FROM _buildedge
),
_buildarea AS (
	SELECT
    	CASE WHEN (SELECT returnpolygon FROM _params) THEN 
    		ST_BuildArea(the_geom)
        ELSE
        	ST_ExteriorRing(ST_BuildArea(the_geom))
        END as the_geom 
    FROM _buildedge
),
_fixdoors AS (
	SELECT 
    	CASE WHEN the_geom IS NOT NULL AND NOT ST_IsEmpty(the_geom) THEN
        	CASE WHEN (SELECT returnpolygon FROM _params) THEN 
                ST_BuildArea(
                    ST_Union(
                        the_geom,  
                        (SELECT the_geom FROM _buildedge)    
                    )
                )
            ELSE
            	st_exteriorring(
                    ST_BuildArea(
                        ST_Union(
                            the_geom,  
                            (SELECT the_geom FROM _buildedge)    
                        )
                    )
                )            
            END
        ELSE
        	the_geom
        END as the_geom
	FROM
    	_builddoors
),
_newedge AS (
	SELECT 
    	CASE WHEN st_geometrytype(the_geom) not in ('ST_LineString') THEN
        	(
            	SELECT 
                	CASE WHEN the_geom IS NULL OR ST_IsEmpty(the_geom) THEN
                    	(SELECT the_geom FROM _fixdoors)
                    ELSE
                    	the_geom  
                    END as the_geom
                FROM _buildarea
            ) 
        ELSE
        	CASE WHEN (SELECT returnpolygon FROM _params) THEN 
            	st_makepolygon(the_geom)
            ELSE
    			the_geom
            END
        END AS the_geom
    FROM 
     _buildedge
)
select 
 st_buffer( 
    	st_buffer(the_geom,-0.0000000000001,'endcap=flat join=bevel'),
    	0.0000000000001,'endcap=flat join=bevel'
 )
FROM _newedge;
$function$
;CREATE OR REPLACE FUNCTION public.sicob_spikeremover(geometry, angle double precision)
 RETURNS geometry
 LANGUAGE plpgsql
 IMMUTABLE
AS $function$
declare
	_out geometry;
    
begin
    if (select st_geometrytype($1)) = 'ST_MultiPolygon' then
    	_out := st_buildarea(  ST_GeomFromText(st_astext($1), 4326));
    else 
        _out := $1;
    end if;
    	
	if st_geometrytype(_out) <> 'ST_Polygon' then
    	RETURN $1;
    end if;       

select st_makepolygon(
        (/*outer ring of polygon*/
        select st_exteriorring(sicob_spikeremovercore(_out,$2)) as outer_ring
          from st_dumprings(_out)where path[1] = 0 
        ),  
		array(/*all inner rings*/
        select st_exteriorring(sicob_spikeremovercore(geom, $2)) as inner_rings
          from st_dumprings(_out) where path[1] > 0) 
) into _out as geom;

/*        
select st_makepolygon(
        (/*outer ring of polygon*/
        select st_exteriorring(sicob_spikeremovercore(_out,$2)) as outer_ring
          from st_dumprings(_out)where path[1] = 0 
        ),  
		array(/*all inner rings*/
        select st_exteriorring(sicob_spikeremovercore(_out, $2)) as inner_rings
          from st_dumprings(_out) where path[1] > 0) 
) into _out as geom;
*/
/*
select st_exteriorring(sicob_spikeremovercore(_out,$2)) into _out as outer_ring
          from st_dumprings(_out)where path[1] = 0 ;
*/
/*
select st_exteriorring(sicob_spikeremovercore(geom, $2)) into _out as inner_rings
          from st_dumprings(_out) where path[1] > 0 ;
*/

RETURN _out;

EXCEPTION
WHEN others THEN
            RAISE EXCEPTION 'geoSICOB-> % (sicob_spikeremover): % (%)',st_astext(_out), SQLERRM, SQLSTATE;	
end;
$function$
 