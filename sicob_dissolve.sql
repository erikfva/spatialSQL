SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_dissolve(_opt json)
 RETURNS json
 LANGUAGE plpgsql
AS $function$
DECLARE 
  a TEXT;
  _fldgroup text;
  _fldgeom text;
  _condition_a text;
  _subfixresult text;
  _schema text;
  _lyrsql text;
  row_cnt integer;

---------------------------------
  sql text;
  lyr_diss text;

 tbl_nameA text; oidA text; sch_nameA text;
 _out json := '{}';
 -------------------------------

BEGIN
---------------------------
--PRE - CONDICIONES
---------------------------
--> Los elementos de las capas de entradas deben ser poligonos (POLYGON).
---------------------------
--PARAMETROS DE ENTRADA
---------------------------
--> lyr_in : capa de poligonos que se desea disolver.
--> condition (opcional): Filtro para los datos de "a". Si no se especifica, se toman todos los registros.
--> subfix (opcional): texto adicional que se agrega al nombre de la capa resultante.
--> schema (opcional): esquema del la BD donde se desea crear la capa resultante. Si no se especifica se crea en "temp".
--> temp (opcional true/false): Indica si la capa resultante sera temporal mientras dura la transaccion. Esto se requiere cuando el resultado es utilizado como capa intermedia en otros procesos dentro de la misma transaccion. Por defecto es FALSE.
--> add_sup (opcionl): Indica si se va a calacular y agregar la superficie (ha) por cada feature.
--> create_index (opcional true/false): Se adiciona al resultado el indice espacial basado en the_geom y el indice del campo sicob_id
---------------------------
--VALORES DEVUELTOS
---------------------------
--> lyr_diss : capa resultante. Solo se incluyen los campos: sicob_id/fldgroup, the_geom/fldgeom.

--_opt := '{"a":"processed.f20171005fgcbdae84fb9ea1_nsi","b":"coberturas.parcelas_tituladas","subfix":"", "schema":"temp"}';

--RAISE NOTICE '_opt: %', _opt::text;

a := (_opt->>'lyr_in')::TEXT;
_lyrsql := COALESCE(_opt->>'lyr_sql',''); 
_condition_a := COALESCE((_opt->>'condition')::text, 'TRUE');

_subfixresult := COALESCE(_opt->>'subfix','diss'); 
_schema := COALESCE(_opt->>'schema','temp');

_fldgroup := COALESCE(_opt->>'fldgroup','');
_fldgeom := COALESCE(_opt->>'fldgeom','the_geom');

IF _lyrsql = '' THEN
	SELECT *,a::regclass::oid FROM sicob_split_table_name(a::text) INTO sch_nameA, tbl_nameA, oidA ;
ELSE
	tbl_nameA := a;
    a := '(' || _lyrsql || ') a ';
END IF;
            sql := '
            WITH
            grouppolygons AS (
                SELECT
                    ' || 
                    CASE WHEN _fldgroup <> '' THEN 
                    	_fldgroup
                    ELSE
                    	'1'
                    END || ' as idgroup,
                    ' || 
                    CASE WHEN _fldgroup <> '' THEN 
					'st_multi(
                        st_collect(
                            ' || _fldgeom || '                
                        )
                    )'
                    ELSE
                    	'the_geom'
                    END || '
                    AS the_geom
                FROM
                    ' || a || '
                WHERE
                (' || _condition_a || ')' ||
                CASE WHEN _fldgroup <> '' THEN 
                'GROUP BY idgroup'
                ELSE
                ''
                END || '    
            ),
            dissolve_area AS (
                SELECT 
                    idgroup, st_union(the_geom) AS the_geom
                FROM grouppolygons
                GROUP BY idgroup 
            )
            SELECT
            	idgroup AS ' || CASE WHEN _fldgroup <> '' THEN _fldgroup ELSE 'sicob_id' END || ', ' ||
                CASE WHEN COALESCE((_opt->>'add_sup')::boolean, FALSE) THEN
                'st_area( ST_Transform(the_geom, SICOB_utmzone_wgs84(the_geom)) )/10000 as sicob_sup,'
                ELSE
                ''
                END ||
                'the_geom AS ' || _fldgeom || '
            FROM dissolve_area;';    

    lyr_diss := tbl_nameA || '_' || _subfixresult;
	IF COALESCE((_opt->>'temp')::boolean, FALSE) THEN
    	EXECUTE 'DROP TABLE IF EXISTS ' || lyr_diss;
    	EXECUTE 'CREATE TEMPORARY TABLE ' || lyr_diss || ' ON COMMIT DROP AS ' || sql;
    ELSE
    	lyr_diss := _schema || '.' || lyr_diss;
        EXECUTE 'DROP TABLE IF EXISTS ' || lyr_diss;
    	EXECUTE 'CREATE UNLOGGED TABLE ' || lyr_diss || ' AS ' || sql;
    END IF;
	GET DIAGNOSTICS row_cnt = ROW_COUNT;    
    IF row_cnt > 0 AND COALESCE((_opt->>'create_index')::boolean,FALSE) = TRUE THEN
         
        
        IF COALESCE((_opt->>'temp')::boolean,FALSE) = TRUE THEN
        	IF sicob_exist_column(lyr_diss, 'sicob_id') THEN
        		EXECUTE 'DROP INDEX IF EXISTS ' || lyr_diss || '_sicob_id';
            	EXECUTE 'CREATE INDEX ' || lyr_diss || '_sicob_id ON ' || lyr_diss || ' USING btree (sicob_id);'; 
        	END IF;
        ELSE
        	IF sicob_exist_column(lyr_diss, 'sicob_id') THEN
        		EXECUTE 'ALTER TABLE ' || lyr_diss || ' ADD PRIMARY KEY (sicob_id);';
            END IF;
            EXECUTE 'DROP INDEX IF EXISTS ' || tbl_nameA || '_' || _subfixresult || '_geomid CASCADE';                 
            EXECUTE 'CREATE INDEX ' || tbl_nameA || '_' || _subfixresult || '_geomid
            ON ' || lyr_diss || ' 
            USING GIST (the_geom) ';             
        END IF;
	END IF;
    
    
    
    _out := jsonb_build_object('lyr_diss',lyr_diss);
    
    IF COALESCE((_opt->>'add_sup')::boolean, FALSE) THEN
    	sql := 'SELECT sum(sicob_sup) as sicob_sup_total FROM ' || lyr_diss;
        _out := _out::jsonb || sicob_executesql(sql,json_build_object('return_scalar',TRUE))::jsonb;
    END IF;
   
    RETURN _out;
    

EXCEPTION
WHEN others THEN
            RAISE EXCEPTION 'geoSICOB (sicob_dissolve) % , % , _opt: % | sql: % ', SQLERRM, SQLSTATE, _opt, sql;	
END;
$function$
 