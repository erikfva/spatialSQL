CREATE OR REPLACE FUNCTION public.sicob_snap_edge3(_opt json)
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
--> subfix (opcional): texto adicional que se agregará al nombre de "a" para formar el nombre de la capa resultante. Si no se especifica por defecto es "_adjusted".
--> schema (opcional): esquema del la BD donde se creará la capa resultante. Si no se especifica se creará en "temp".
--> tolerance: Distancia máxima (en metros) para el autoajuste automático de los bordes de "a" hacia los bordes de "b" (snapping). Si no se especifica, no se realiza autoajuste y la funcion devolvera "a". 
--> temp (opcional true/false) : Indica si la capa resultante será temporal mientras dura la transacción. Esto se requiere cuando el resultado es utilizado como capa intermedia en otros procesos dentro de la misma transacción. Por defecto es FALSE.


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
 