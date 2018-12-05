SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_snap_edge(_opt json)
 RETURNS json
 LANGUAGE plpgsql
 COST 200
AS $function$
DECLARE 
  a TEXT;
  b TEXT;
  _condition_a text; _condition_b text;
  _subfixresult text;
  _schema text;
  _tolerance double precision;

---------------------------------
  
 sql text;

 tbl_nameA text; sch_nameA text;
 tbl_nameB text; sch_nameB text;
 
 row_cnt integer;
 __a text; __b text; a__b text;
 _out json := '{}';
 -------------------------------

BEGIN
---------------------------
--PRE - CONDICIONES
---------------------------
--> Los elementos de las capas de entradas deben ser poligonos simples (POLYGON). No deben ser Multipolygon o colecciones.
--> El campo identificador unico de cada capa debe ser "sicob_id".
---------------------------
--PARAMETROS DE ENTRADA
---------------------------
--> a : capa de poligonos cuyos bordes se ajustaran a los bordes de la capa "b".
--> condition_a (opcional): Filtro para los datos de "a". Si no se especifica, se toman todos los registros.
--> b : capa de poligonos a cuyos bordes se ajustaran los poligonos de "a".
--> condition_b (opcional): Filtro para los datos de "b". Si no se especifica, se toman todos los registros.
--> subfix (opcional): texto adicional que se agregarÃ¯Â¿Â½ al nombre de "a" para formar el nombre de la capa resultante. Si no se especifica por defecto es "_adjusted".
--> schema (opcional): esquema del la BD donde se crearÃ¯Â¿Â½ la capa resultante. Si no se especifica se crearÃ¯Â¿Â½ en "temp".
--> tolerance: Distancia mÃ¯Â¿Â½xima (en metros) para el autoajuste automÃ¯Â¿Â½tico de los bordes de "a" hacia los bordes de "b" (snapping). Si no se especifica, no se realiza autoajuste y la funcion devolvera "a". 
--> temp (opcional true/false) : Indica si la capa resultante serÃ¯Â¿Â½ temporal mientras dura la transacciÃ¯Â¿Â½n. Esto se requiere cuando el resultado es utilizado como capa intermedia en otros procesos dentro de la misma transacciÃ¯Â¿Â½n. Por defecto es FALSE.


---------------------------
--VALORES DEVUELTOS
---------------------------
--> lyr_adjusted : capa resultante del ajuste de bordes. Solo se incluyen dos campos: sicob_id,the_geom.

--_opt := '{"a":"processed.f20171005fgcbdae84fb9ea1_nsi","b":"coberturas.parcelas_tituladas","subfix":"_adjusted","tolerance":"5.3", "schema":"temp"}';

a := (_opt->>'a')::TEXT;
_condition_a := COALESCE((_opt->>'condition_a')::text, 'TRUE');

b := (_opt->>'b')::TEXT;
_condition_b := COALESCE((_opt->>'condition_b')::text, 'TRUE');

_subfixresult := COALESCE(_opt->>'subfix','_adjusted'); 
_schema := COALESCE(_opt->>'schema','temp');
_tolerance := COALESCE((_opt->>'tolerance')::real, 0);

SELECT * FROM sicob_split_table_name(a::text) INTO sch_nameA, tbl_nameA;
SELECT * FROM sicob_split_table_name(b::text) INTO sch_nameB, tbl_nameB;

--RAISE NOTICE 'sicob_snap_edge: %', _opt::text;

    IF _tolerance > 0 THEN
        --> CREANDO LOS PARES de indices (ai,bi) DE LOS POLIGONOS QUE SE INTERSECTAN
        a__b := 'a__b';

        sql := '
        SELECT DISTINCT a.sicob_id as id_a, b.sicob_id as id_b
        FROM 
        ' || b::text || ' b INNER JOIN ' || a::text || ' a
        ON (
            ' || _condition_a || ' AND ' || _condition_b || ' AND st_intersects(a.the_geom, b.the_geom) AND NOT ST_Touches(a.the_geom, b.the_geom)
        )';

        RAISE DEBUG 'Running %', sql;
        EXECUTE 'DROP TABLE IF EXISTS ' || a__b;
        EXECUTE 'CREATE TEMPORARY TABLE ' || a__b || ' ON COMMIT DROP AS ' || sql;

        GET DIAGNOSTICS row_cnt = ROW_COUNT; -->obteniendo la cantidad de intersecciones.	
        IF row_cnt > 0 THEN
            sql := 'DROP INDEX IF EXISTS ' || a__b || '_a; DROP INDEX IF EXISTS ' || a__b || '_b;';
            RAISE DEBUG 'Running %', sql;
            EXECUTE sql;
                    
            sql := 'CREATE INDEX ' || a__b || '_a
                    ON ' || a__b || ' 
                    USING btree (id_a); 
                    CREATE INDEX ' || a__b || '_b
                    ON ' || a__b || ' 
                    USING btree (id_b);';
            RAISE DEBUG 'Running %', sql;
            EXECUTE sql; 
        END IF;
    END IF;

    IF _tolerance = 0 OR row_cnt = 0 THEN
    --> Retornar sin hacer ajustes.
    	sql := '';
        IF _condition_a <> 'TRUE' THEN
        	sql := 'SELECT a.sicob_id, a.the_geom FROM ' || a || ' a WHERE ' || _condition_a;
        END IF;
	ELSE
        -----------------------------------------
        --AJUSTANDO LOS BORDES DE a HACIA b 
        --EN UNA DISTANCIA APROXIMADA A _tolerance (en metros)
        -----------------------------------------
           
        sql := '
        	SELECT DISTINCT inters.id_a as sicob_id, a.the_geom
            FROM ' || a__b || ' inters 
            INNER JOIN ' || a::text || ' a ON (a.sicob_id = inters.id_a)
            INNER JOIN ' || b::text || ' b ON (b.sicob_id = inters.id_b)
            WHERE (ST_CoveredBy(a.the_geom,b.the_geom) = TRUE )';
            
        --EXECUTE 'CREATE TEMPORARY TABLE poly_fullcovered ON COMMIT DROP AS ' || sql;
        EXECUTE 'DROP TABLE IF EXISTS ' || tbl_nameA || '_fullcovered';
        EXECUTE 'CREATE TEMPORARY TABLE ' || tbl_nameA || '_fullcovered ON COMMIT DROP AS ' || sql;
  
        sql := '
              SELECT DISTINCT inters.id_a as sicob_id,
              (SELECT the_geom FROM ' || a::text || ' a WHERE a.sicob_id = inters.id_a) as the_geom,
              (SELECT ST_Collect(b.the_geom) FROM ' || a__b || ' a__b INNER JOIN ' || b::text || ' b ON(a__b.id_a = inters.id_a AND b.sicob_id = a__b.id_b) ) as target
              FROM ' || a__b || ' inters
              WHERE NOT EXISTS (SELECT sicob_id FROM ' || tbl_nameA || '_fullcovered t WHERE t.sicob_id = inters.id_a) 
              ';
        EXECUTE 'DROP TABLE IF EXISTS ' || tbl_nameA || '_partialcovered';
        EXECUTE 'CREATE TEMPORARY TABLE ' || tbl_nameA || '_partialcovered ON COMMIT DROP AS ' || sql; 
        
        sql := '
            SELECT 
                sicob_id, 
                CASE WHEN ST_NRings(the_geom) > 1 THEN
                        sicob_update_exteriorring( 
                            sicob_snap_edge(
                                st_exteriorring(the_geom),
                                st_makevalid(  st_collectionextract( target, 3) ),
                                ''{"tolerance":"' || _tolerance::text || '"}''
                            ), 
                            the_geom, 
                            ''{}''
                        )
                ELSE
                        sicob_snap_edge(
                            st_exteriorring(the_geom),
                            st_makevalid(  st_collectionextract( target, 3) ),
                            ''{"tolerance":"' || _tolerance::text || '","returnpolygon":true}''
                        )

                END
                AS the_geom
            FROM ' || tbl_nameA || '_partialcovered a
              ';             
       EXECUTE 'DROP TABLE IF EXISTS ' || tbl_nameA || '_snapping'; 
       EXECUTE 'CREATE TEMPORARY TABLE ' || tbl_nameA || '_snapping ON COMMIT DROP AS ' || sql; 
   
        sql := '       
          SELECT t.* 
          FROM 
          (
              SELECT sicob_id,the_geom 
              FROM ' || a::text || ' a 
              WHERE ' || _condition_a || ' AND NOT EXISTS (SELECT sicob_id FROM ' || a__b || ' a__b WHERE a__b.id_a = a.sicob_id)
              UNION ALL
              SELECT * FROM ' || tbl_nameA || '_fullcovered
              UNION ALL
              SELECT * FROM ' || tbl_nameA || '_snapping
          )
          t ORDER BY t.sicob_id    
        ';   
        RAISE DEBUG 'Running %', sql;  
    END IF; 
   
    IF sql = '' THEN
    	--> Si no se genero una nueva cobertura devuelve la misma de entrada.
    	RETURN ('{"lyr_adjusted":"' || a || '"}')::json;
    END IF;
    
    __a := tbl_nameA || _subfixresult;
	IF COALESCE((_opt->>'temp')::boolean, FALSE) THEN
    	EXECUTE 'DROP TABLE IF EXISTS ' || __a;
    	EXECUTE 'CREATE TEMPORARY TABLE ' || __a || ' ON COMMIT DROP AS ' || sql;
    ELSE
    	__a := _schema || '.' || __a;
        EXECUTE 'DROP TABLE IF EXISTS ' || __a;
    	EXECUTE 'CREATE UNLOGGED TABLE ' || __a || ' AS ' || sql;
    END IF;
    
    EXECUTE 'ALTER TABLE ' || __a || ' ADD PRIMARY KEY (sicob_id);';
    /*
    sql := 'DROP INDEX IF EXISTS ' || tbl_nameA || _subfixresult || '_geomidx';
    RAISE DEBUG 'Running %', sql;
    EXECUTE sql;
            
    sql := 'CREATE INDEX ' || tbl_nameA || _subfixresult || '_geomidx
    ON ' || __a || ' 
    USING GIST (the_geom) ';
    RAISE DEBUG 'Running %', sql;
    EXECUTE sql;    
    */
    
    RETURN ('{"lyr_adjusted":"' || __a || '"}')::json;
    

EXCEPTION
WHEN others THEN
            RAISE EXCEPTION 'geoSICOB (sicob_snap_edge) % , % , _opt: % | sql: %', SQLERRM, SQLSTATE, _opt, sql;	
END;
$function$
;CREATE OR REPLACE FUNCTION public.sicob_snap_edge(_edge geometry, _opt json)
 RETURNS geometry
 LANGUAGE plpgsql
 STABLE
AS $function$
DECLARE 
    sql text;
    _newedge geometry;
    _buffer2snap geometry;
    _exterior_ring geometry;
    row_cnt integer;
    _tolerance double precision;
    _returnpolygon boolean;
BEGIN
---------------------------
--PARAMETROS DE ENTRADA
---------------------------
-->	_edge: linea del borde que serÃ¯Â¿Â½ ajustada hacia "target".
-->	_opt : cadena json con las sigtes. opciones :
-->		target : Capa con los poligono a cuyos bordes se ajustara "_edge".
-->		condition : Filtro para la capa "target" (anteponer el prefijo "b.").
-->		tolerance : Distancia mÃ¯Â¿Â½xima(una aproximaciÃ¯Â¿Â½n a metros) para el ajuste.
-->		returnpolygon (true/false): Devuelve el resultado convertido a polÃ¯Â¿Â½gono. Si no se especifica devuelve una linea de borde.
-->		  


/*
SELECT ST_ExteriorRing(
		(ST_Dump(the_geom)).geom) into _edge
    FROM  processed.f20160708agfbdecf223329c_nsi_pred  --processed.f20160929daebgcf1933cf73_nsi 
WHERE (titulo IS NULL) and sicob_id = 36;

_opt = ('{"target":"coberturas.predios_proceso_geosicob_geo_201607", "tolerance":"' || (6::float/111000)::text || '"}')::json;
*/

/*
SELECT ST_ExteriorRing(
		(ST_Dump(the_geom)).geom) into _edge
    FROM  processed.f20160708agfbdecf223329c_nsi
WHERE sicob_id = 2;

_opt = ('{"target":"coberturas.predios_titulados", "tolerance":"' || (5.3::float/111000)::text || '"}')::json;

_opt = ('{"target":"coberturas.predios_proceso_geosicob_geo_201607", "tolerance":"' || (5.3::float/111000)::text || '","returnpolygon":true}')::json;
*/

/*
_edge := 'LINESTRING(-62.4419591643761 -17.6335777505114,-62.441930518342 -17.638976274835,-62.4421415649626 -17.6389944729068,-62.4419591643761 -17.6335777505114)'::geography::geometry;
_opt = ('{"target":"coberturas.predios_titulados", "tolerance":"5.3","returnpolygon":true}')::json;
*/

/*
edge := 'LINESTRING(-64.4777013405345 -17.258590881422,-64.4778663912426 -17.2588607723101,-64.4778663912433 -17.2588607723113,-64.4778663912464 -17.2588607723164,-64.4790217631241 -17.2607500035688,-64.4797125421763 -17.2618795232381,-64.4748255976107 -17.2606597905827,-64.4748255976095 -17.2606597905823,-64.473552392031 -17.260341988782,-64.4733259883332 -17.2599717573221,-64.473325988331 -17.2599717573185,-64.4729958977553 -17.259431967076,-64.4728430558536 -17.2591820259612,-64.4728430558493 -17.2591820259541,-64.4728430558044 -17.2591820258808,-64.4728308531744 -17.2591620716114,-64.472005639011 -17.2578125920417,-64.4719854667802 -17.2577796035334,-64.4719854667598 -17.2577796035,-64.4719854667577 -17.2577796034967,-64.4718405974811 -17.2575426955685,-64.4716755564602 -17.2572727989279,-64.4714137482343 -17.2568446534158,-64.471413748232 -17.256844653412,-64.4714137482265 -17.256844653403,-64.4713454759115 -17.2567330050898,-64.4712754668683 -17.2566185154429,-64.47225240566 -17.2568623868996,-64.4722524056632 -17.2568623869004,-64.4731005375204 -17.2570740996373,-64.4745140943076 -17.2574269468055,-64.4749269574206 -17.2575300022177,-64.4749269574233 -17.2575300022184,-64.4759276557105 -17.2577797836047,-64.4759820486862 -17.2577933604584,-64.4759820487037 -17.2577933604627,-64.4774354730658 -17.2581561348381,-64.4777013405345 -17.258590881422)'::geography::geometry;
_opt = ('{"target":"coberturas.parcelas_tituladas", "tolerance":"5.3","returnpolygon":true}')::json;
*/
	_tolerance := (_opt->>'tolerance')::real / 111000 ;
    _returnpolygon := COALESCE((_opt->>'returnpolygon')::boolean,false);
   
    sql := '
    SELECT b.the_geom
    FROM 
        ' || (_opt->>'target')::text || ' b 
    WHERE ' || COALESCE((_opt->>'condition')::text ,'TRUE') ||
    ' AND st_intersects(b.the_geom, $1) ';

	sql := 'SELECT  ST_Collect(ST_ExteriorRing(the_geom)) 
            FROM (
                SELECT (dp).path[1] as gid, (dp).geom as the_geom 
                FROM(
                    SELECT st_dump(the_geom) as dp
                    FROM 
                    (' || sql || ') ___b                 
                ) t        
            ) As u
            GROUP BY gid';

    RAISE DEBUG 'Running %', sql;
    EXECUTE sql INTO _exterior_ring USING _edge; 

WITH
/*
_params AS (
	SELECT
    	(
        	SELECT st_union(ST_Buffer(_edge, tolerance )) FROM _params_in 
        ) as buffer_snap,
        _edge,
        (
        	SELECT  ST_Collect(ST_ExteriorRing(the_geom)) 
            FROM (
                SELECT (dp).path[1] as gid, (dp).geom as the_geom 
                FROM(
                    SELECT st_dump(the_geom) as dp
                    FROM 
                    ___b                 
                ) t        
            ) As u
            GROUP BY gid      	
        ) as exterior_ring,
        tolerance,
        returnpolygon
    FROM _params_in
),
*/
_params AS (
	SELECT
	st_union(ST_Buffer(_edge, _tolerance )) as buffer_snap,
    _edge as edge,
    _exterior_ring as exterior_ring,
    _tolerance as tolerance,
    _returnpolygon as returnpolygon
),
line_b AS (
	SELECT 
		st_intersection(exterior_ring,buffer_snap) as the_geom
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
              pt,(SELECT _edge FROM _params)
          )
          as direction_line,
          sicob_shadowline(
              (SELECT the_geom FROM topo_face1 WHERE segment = t.segment), 
              ST_ShortestLine(
                  pt,(SELECT _edge FROM _params)
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
    	(SELECT _edge FROM _params), 
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
		(SELECT _edge FROM _params), the_geom
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
    ,0.0000001 ) as the_geom
    FROM _shadowa
),
_clipa AS (    
	SELECT row_number() over() as segmenta,(dmp).geom as the_geom, 
		st_startpoint((dmp).geom) as pini, st_endpoint((dmp).geom) as pfin
	FROM(
 		SELECT st_dump( st_linemerge(st_difference( (SELECT _edge FROM _params limit 1) , m.the_geom )) ) as dmp 
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
		(SELECT _edge FROM _params)
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
select the_geom INTO _newedge 
FROM _newedge; 
   
	RETURN _newedge;     

EXCEPTION
WHEN others THEN
            RAISE EXCEPTION 'geoSICOB (sicob_snap_edge): _edge: % >> _opt: % >> _newedge: % , (%), newedge: %', st_astext(_edge), _opt::text, st_astext(_newedge), SQLERRM, SQLSTATE;	
END;
$function$
;CREATE OR REPLACE FUNCTION public.sicob_snap_edge(_edge geometry, _boundaries geometry, _opt json)
 RETURNS geometry
 LANGUAGE sql
 STRICT
AS $function$
WITH
edge AS (
    SELECT _edge as the_edge
),
boundaries AS (
    SELECT   _boundaries  as boundaries
),
_params AS (
SELECT 
    edge,
    (SELECT st_union(ST_Buffer(the_edge, tolerance )) FROM edge)  as buffer_snap,
    tolerance,
    returnpolygon,
    boundaries
 FROM (
 	SELECT 
    (SELECT the_edge FROM edge) as edge,
    (SELECT boundaries FROM boundaries) as boundaries,
    COALESCE((_opt->>'tolerance')::REAL, 0 ) as tolerance,
    COALESCE((_opt->>'returnpolygon')::boolean,TRUE) as returnpolygon
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
SELECT 
	the_geom
FROM _newedge;
$function$
 