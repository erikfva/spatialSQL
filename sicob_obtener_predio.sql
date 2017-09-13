CREATE OR REPLACE FUNCTION public.sicob_obtener_predio(_opt json)
 RETURNS json
 LANGUAGE plpgsql
AS $function$
DECLARE 
 sql text;
 _out json := '{}';
 _tolerance text;
 _geojson text;
 _min_sup float;
 _row_cnt integer;
 tbl_name text;
 _lyr_pred text;
 _fldpredio text;
 _fldpropietario text;
 _condition text;
 a text;
 createdResult boolean := FALSE;
 lyrs_predio json;
 lyr_predio json;
 tmp json;
 inters_cnt integer; diff_cnt integer;
BEGIN

--VARIABLES DE ENTRADA:
--	lyr_in : capa con los poligonos que deben ser ubicados en sus predios correspondientes
--	condition (opcional):  Filtro para los datos de la capa "lyr_in". Si no se especifica, se toman todos los registros.
--> lyr_pred : capa con predios de referencia.
--> lyr_parc : capa con las parcelas de referencia.
--> fldpredio : campo que contiene el nombre del predio en la capa de referencia.
--> fldpropietario : campo que contiene el nombre del propietario en la capa de referncia. 
--> geojson (opcional true/false): Devuelve el resultado en formato geojson. Por defecto es false.
--> min_sup (opcional): Superficie minima (en hectareas) permitida para los poligonos de la capa resultado por defecto es igual a 0.002 que fue la minima encontrada en los PDM.
--> tolerance (opcional): Distancia máxima (en metros) para el autoajuste automático de los bordes de "a" hacia los bordes de "b" (snapping). Si no se especifica, no se realiza autoajuste.

--VARIABLES DE SALIDA:
--> lyr_over: Nombre de la capa resultante, este es igual al nombre de la capa de entrada más el subfijo “_ppred”. Esta capa siempre será creada en el esquema “temp” de la BD. 
--> features_inters_cnt: Cantidad de polígonos que SI se han encontrado sus predios.
--> features_diff_cnt: Cantidad de polígonos de los cuales NO se ha encontrado predio.
--> lyr_geojson : Si se ha indicado el valor geojson de entrada como "true", devuelve la capa resultado en formato geojson.
--> lyr_pred: Nombre de la capa que contiene los predios encontrados.
--> predios : Array json con el detalle de los predios encontrados.
--> total_predios: Cantidad de predios encontrados.
 
--_opt := '{"lyr_in":"processed.f20161118dgfaceb9e93b41b_nsi"}'::json;
--_opt := '{"lyr_in":"coberturas.b0603206_pdm_a052016","condition":"objectid_1 = 35884"}';
--_opt := '{"lyr_in":"coberturas.b0603206_pdm_a052016","condition":"objectid_1 = 11426"}';
--_opt := '{"lyr_in":"coberturas.pdm","condition":"res_adm = ''RS-OLSC-PDM-129-2000''"}';
--_opt := '{"lyr_in":"uploads.f20170704gcfebdac5d7c097","lyr_parc":"uploads.f20170705ecgdbfafbc4c20f"}'::json;
--_opt := '{"lyr_in":"processed.f20170718fagebdcf580ac83_nsi"}'::json;

_tolerance := COALESCE((_opt->>'tolerance')::text, '5.3');
tbl_name := (_opt->>'lyr_in')::text;
	SELECT (sicob_split_table_name(tbl_name)).table_name INTO tbl_name ;
_condition := COALESCE( (_opt->>'condition')::text , 'TRUE');

lyrs_predio := '[{
    	"subfix":"_tit",
        "lyr_parc":{
        	"source":"coberturas.parcelas_tituladas",
            "fldidpredio":"idpredio",
            "fldpredio":"predio",
            "fldpropietario":"propietario"
        },
        "lyr_pred":{
        	"source":"coberturas.predios_titulados",
            "fldidpredio":"idpredio"
        }
    },
	{
    	"subfix":"_tioc",
        "tolerance":"0",
        "lyr_parc":{
        	"source":"coberturas.tioc",
            "fldidpredio":"idpredio",
            "fldpredio":"nomparcela",
            "fldpropietario":"nomparcela"
        },
        "lyr_pred":{
        	"source":"coberturas.predios_tioc",
            "fldidpredio":"idpredio"
        }
    },
	{
    	"subfix":"_pop",
        "tolerance":"0",
        "lyr_parc":{
        	"source":"coberturas.predios_pop",
            "fldpredio":"nom_pre",
            "fldpropietario":"nom_pro"
        }
    },
	{
    	"subfix":"_proc",
        "tolerance":"0",
        "lyr_parc":{
        	"source":"coberturas.predios_proceso_geosicob_geo_201607",
            "fldidpredio":"idpredio",
            "fldpredio":"nompred",
            "fldpropietario":"beneficiar"
        },
        "lyr_pred":{
        	"source":"coberturas.predios_referenciales",
            "fldidpredio":"sicob_id"
        }
	}]'::json;
                 
IF COALESCE( (_opt->>'lyr_parc')::text,'') <> '' THEN --> Si se indica cobertura de referencia.
	_fldpredio := COALESCE((_opt->>'fldpredio_parc')::text, 'predio');
   	IF NOT sicob_exist_column(_opt->>'lyr_parc', _fldpredio ) THEN
       	_fldpredio := '''COLUMNA NO ENCONTRADA''';
   	END IF;
    _fldpropietario := COALESCE((_opt->>'fldpropietario_parc')::text, 'propietario');
   	IF NOT sicob_exist_column(_opt->>'lyr_parc', _fldpropietario ) THEN
      	_fldpropietario := '''COLUMNA NO ENCONTRADA''';
   	END IF;
	lyr_predio :=   ('{
                        "lyr_parc":{"source":"' || (_opt->>'lyr_parc')::text || '",
                        "fldpredio":"' || _fldpredio || '",
                        "fldpropietario":"' || _fldpropietario || '"},
                        "subfix":"_parc"}')::json;

    lyrs_predio := (lyr_predio::jsonb || lyrs_predio::jsonb)::json;

END IF;

inters_cnt := 0;                  
a := (_opt->>'lyr_in')::text;  
--> ANALIZANDO CADA COBERTURAS DE PREDIOS/PARCELAS 
FOR lyr_predio IN SELECT * FROM json_array_elements(lyrs_predio) LOOP
	IF a <> '' AND lyr_predio->>'lyr_parc' <> '' THEN --> Si existen poligonos para localizar.
    	_out := sicob_overlap(('{"a":"' || a || 
        			'","condition_a":"' || _condition || 
                    '","b":"' || (lyr_predio->'lyr_parc'->>'source')::text || 
                    '","subfix":"' || (lyr_predio->>'subfix')::text || 
                    '","tolerance":"' || COALESCE((lyr_predio->>'tolerance')::text, _tolerance ) || 
                    '","add_diff":true,"temp" : true' || 
                    '}')::json); 
        IF COALESCE( (_out->>'features_inters_cnt')::int,0) > 0 THEN  --> Si se han localizado predios.
        
        	inters_cnt := inters_cnt +  (_out->>'features_inters_cnt')::int;
       
        	IF a <> (_opt->>'lyr_in')::text THEN --> Actualizar referencia de la tabla resultado.
                -->Cambiando la referencia "id_a" de "a" hacia la tabla de entrada "_lyr_in"
                EXECUTE format('
                    UPDATE
                        %s a
                    SET
                        id_a = (	
                            SELECT 
                              b.id_a
                            FROM
                              %s b
                            WHERE
                                b.sicob_id = a.id_a
                            LIMIT 1
                        ),
                        source_a = ''%s''
                ',_out->>'lyr_over',a, (_opt->>'lyr_in')::text);
            END IF;
            
        	_fldpredio := COALESCE((lyr_predio->'lyr_parc'->>'fldpredio')::text, 'predio');
        	_fldpropietario := COALESCE((lyr_predio->'lyr_parc'->>'fldpropietario')::text, 'propietario');
            
            sql := format('
                    SELECT 
                    id_a,
                    source_a,
                    id_b,
                    source_b,            
                    CAST(%s AS text) as predio,
                    CAST(%s AS text) as propietario, ' ||
                    CASE (lyr_predio->>'subfix')::text WHEN '_tit' 
                    	THEN 'titulo' 
                        ELSE 'CAST(NULL AS text)' 
                    END || ' as titulo, ' ||
                    CASE (lyr_predio->>'subfix')::text WHEN '_tit' 
                    	THEN 'to_char(fecha_titulo,''DD/MM/YYYY'')' 
                        ELSE 'CAST(NULL AS TEXT)' 
                    END || ' AS fecha_titulo, ' || 
                    CASE (lyr_predio->>'subfix')::text 
                    	WHEN '_tit' THEN 'tipo_propiedad'
                        WHEN '_tioc' THEN 'CAST(''Territorio Indígena Originario Campesino'' as text)' 
                        ELSE 'CAST(NULL AS text)' 
                    END || ' AS tipo_propiedad, ' ||
                    CASE (lyr_predio->>'subfix')::text 
                    	WHEN '_parc'	THEN 'sicob_sup'
                        WHEN '_tit'	THEN 'sup_predio'
                        WHEN '_pop' THEN 'CASE sup_pre > 0 WHEN TRUE THEN sup_pre ELSE sicob_sup END' 
                        WHEN '_proc' THEN 'sicob_sup' 
                        ELSE 'CAST(NULL AS float)' 
                    END || ' AS sup_predio, ' ||
                    CASE (lyr_predio->>'subfix')::text 
                    	WHEN '_parc'	THEN _fldpredio || '::text'
                    	ELSE 'CAST(NULL AS text)'
                    END || ' AS parcela, ' ||
                    CASE (lyr_predio->>'subfix')::text WHEN '_pop' 
                    	THEN 'res_adm' 
                        ELSE 'CAST(NULL AS text)' 
                    END || ' AS resol_pop, ' ||
                    CASE (lyr_predio->>'subfix')::text WHEN '_pop' 
                    	THEN 'to_char(fec_res,''DD/MM/YYYY'')' 
                        ELSE 'CAST(NULL  AS text)' 
                    END || ' AS fec_resol_pop, ' ||
                    'CAST(''' || COALESCE((lyr_predio->'lyr_pred'->>'source')::text, (lyr_predio->'lyr_parc'->>'source')::text ) || ''' AS text) AS source_predio, ' ||
                    COALESCE((lyr_predio->'lyr_parc'->>'fldidpredio')::text, 'id_b' ) || ' AS id_predio, ' ||
                    'the_geom
                    FROM
                        %s
                    WHERE id_b IS NOT NULL         
                    ',_fldpredio,_fldpropietario, (_out->>'lyr_over')::text);
                    
            RAISE DEBUG 'Running %', sql;
        	IF createdResult THEN
				EXECUTE 'INSERT INTO ' || tbl_name || '_ppred ' || sql;
            ELSE
                EXECUTE 'CREATE TEMPORARY TABLE ' || tbl_name || '_ppred' || ' ON COMMIT DROP AS ' || sql;
                createdResult := TRUE;
        	END IF;
          
            IF COALESCE( (_out->>'features_diff_cnt')::int,0) > 0 THEN --> si existen poligonos NO encontrados.
            	a := (_out->>'lyr_over')::text;
                _condition := 'id_b IS NULL';
            ELSE
            	a := '';
            END IF; 
		END IF;
    END IF;
END LOOP;

--> ADICIONANDO LOS POLIGONOS SIN PREDIO
IF COALESCE( (_out->>'features_diff_cnt')::int,0) > 0 THEN --> si existen poligonos NO encontrados.
  sql := format('
      SELECT 
      id_a,
      source_a,
      id_b,
      source_b,            
      NULL as predio,
      NULL as propietario, 
      CAST(NULL AS text) AS titulo, 
      CAST(NULL AS TEXT) AS fecha_titulo, 
      CAST(NULL AS text) AS tipo_propiedad,
      CAST(NULL AS float) AS sup_predio,
      CAST(NULL AS text) AS parcela,
      CAST(NULL AS text) AS resol_pop, 
      CAST(NULL  AS text) AS fec_resol_pop,
      CAST(NULL AS text) AS source_predio,
      CAST(NULL AS integer) AS id_predio, 
      the_geom
      FROM
          %s
      WHERE id_b IS NULL         
      ',(_out->>'lyr_over')::text);
      RAISE DEBUG 'Running %', sql;
      IF createdResult THEN
          EXECUTE 'INSERT INTO ' || tbl_name || '_ppred ' || sql;
      ELSE
          EXECUTE 'CREATE TEMPORARY TABLE ' || tbl_name || '_ppred' || ' ON COMMIT DROP AS ' || sql;
          createdResult := TRUE;
      END IF;
END IF;

	--> CREANDO LA COBERTURA RESULTANTE Y AGREGANDO LOS CAMPOS DE LA CAPA DE ENTRADA.
    sql := 'SELECT  
  		r.predio,
  		r.propietario,
		r.titulo,
		r.fecha_titulo,
  		r.sup_predio,
  		r.tipo_propiedad,
        r.parcela,
  		r.resol_pop,
  		r.fec_resol_pop,
        cast(r.source_b as text) as source_parcela,
        r.id_b as id_parcela,
        cast(r.source_predio as text) as source_predio,
        r.id_predio, ' || 
    	sicob_no_geo_column(
        	(_opt->>'lyr_in') ,
            '{sicob_id, predio, propietario, titulo, fecha_titulo, sup_predio, tipo_propiedad, parcela, resol_pop, fec_resol_pop, sicob_sup, sicob_utm, id_predio, source_parcela, id_parcela, source_predio}',
        	--> ('{' || sicob_no_geo_column((_opt->>'lyr_over')::text ,'{}',' '::text) || '}')::text[],
            'a.'
        ) || 
        ',r.the_geom
    	 FROM ' || tbl_name || '_ppred' || ' r 
  		 INNER JOIN ' || (_opt->>'lyr_in')::text || ' a ON (r.id_a = a.sicob_id) ' ||
--        'WHERE r.predio IS NOT NULL ' || --> Filtrando en la capa resultado solamente los poligonos que tienen predio.
        'ORDER BY a.sicob_id';
	sql := '
    	SELECT CAST(row_number() OVER () AS integer) AS sicob_id, t.* 
        FROM 
        (' || sql || ') t';
    EXECUTE 'CREATE TEMPORARY TABLE ' || tbl_name || '_ppred1' || ' ON COMMIT DROP AS ' || sql;
    
	a := 'temp.' || tbl_name || '_ppred'; --> Nombre de la capa resultante de intersectar los poligonos de entrada con los predios.
--> Complementando información de ubicación politico-administrativo.
    sql := 'SELECT ' || 
    sicob_no_geo_column(
        	tbl_name || '_ppred1' ,
            '{nom_dep,nom_prov,nom_mun}',
            't.'
        )
    || ',u.nom_dep, u.nom_prov, u.nom_mun, t.the_geom 
    	FROM
        	' || tbl_name || '_ppred1' || ' t LEFT OUTER JOIN
			(SELECT * FROM sicob_ubication(''' || tbl_name || '_ppred1' || ''') ) u
			ON (u.sicob_id = t.sicob_id)
		';  
	EXECUTE 'DROP TABLE IF EXISTS ' || a; 
    EXECUTE 'CREATE TABLE ' || a || ' AS ' || sql;
    
--> CREANDO INDICE GEOGRAFICO
    sql := 'CREATE INDEX ' || tbl_name || '_ppred_geomidx
	ON ' || a || '  
	USING GIST (the_geom) ';
	RAISE DEBUG 'Running %', sql;
	EXECUTE sql;
   
IF inters_cnt > 0 THEN --> Si se han encontrado poligonos.               
--> AGREGANDO INFORMACION DE POP A LOS POLIGONOS TITULADOS Y POLIGONOS UBICADOS EN PARCELAS DE REFERENCIA
	_condition := 'source_parcela = ''coberturas.parcelas_tituladas''';    
	IF COALESCE( (_opt->>'lyr_parc')::text,'') <> '' THEN --> Si se indica cobertura de referencia.
    	_condition := _condition || ' OR source_parcela = ''' || (_opt->>'lyr_parc')::text || '''';
    END IF;
    tmp := sicob_overlap(('{"a":"' || a || '","condition_a":"' || _condition || '","b":"coberturas.predios_pop","subfix":"_pop","tolerance":"0","add_diff":false, "temp": true}')::json);
    IF COALESCE( (tmp->>'features_inters_cnt')::int,0) > 0 THEN --> Si se han encontrado POP.
        sql := '
            UPDATE ' || a || ' a
            SET resol_pop = b.res_adm, fec_resol_pop = b.fec_res
            FROM ' || (tmp->>'lyr_over')::text || ' b
            WHERE
            b.id_a = a.sicob_id
        ';
        RAISE DEBUG 'Running %', sql;
        EXECUTE sql;            
    END IF;
    
--> AGREGANDO INFORMACION DE TITULACION A LOS POLIGONOS UBICADOS EN PARCELAS DE REFERENCIA
	IF COALESCE( (_opt->>'lyr_parc')::text,'') <> '' THEN --> Si se indica cobertura de referencia.
    	_condition := 'source_parcela = ''' || (_opt->>'lyr_parc')::text || '''';
        tmp := sicob_overlap(('{"a":"' || a || '","condition_a":"' || _condition || '","b":"coberturas.parcelas_tituladas","subfix":"_tit","tolerance":"0","add_diff":false, "temp": true}')::json);
        IF COALESCE( (tmp->>'features_inters_cnt')::int,0) > 0 THEN --> Si se han encontrado parcelas tituladas.
            sql := '
                UPDATE ' || a || ' a
                SET titulo = b.titulo, fecha_titulo = b.fecha_titulo, tipo_propiedad = b.tipo_propiedad,
                predio = CASE WHEN round ( abs(b.sup_predio - a.sup_predio)*100/b.sup_predio) < 5 THEN b.predio ELSE a.predio END,
                propietario = CASE WHEN round ( abs(b.sup_predio - a.sup_predio)*100/b.sup_predio) < 5 THEN b.propietario ELSE a.propietario END,
                sup_predio = CASE WHEN round ( abs(b.sup_predio - a.sup_predio)*100/b.sup_predio) < 5 THEN b.sup_predio ELSE a.sup_predio END,
                source_predio = CASE WHEN round ( abs(b.sup_predio - a.sup_predio)*100/b.sup_predio) < 5 THEN ''coberturas.predios_titulados'' ELSE a.source_predio END,
                id_predio = CASE WHEN round ( abs(b.sup_predio - a.sup_predio)*100/b.sup_predio) < 5 THEN b.idpredio ELSE a.id_predio END
                FROM (
                  SELECT id_a, min(idpredio) as idpredio, string_agg(predio , '', '') as predio, string_agg(propietario , '', '') as propietario, string_agg(titulo , '', '') as titulo, string_agg(to_char(fecha_titulo,''DD/MM/YYYY'') , '', '') as fecha_titulo, min(tipo_propiedad) as tipo_propiedad, sum(sup_predio) as sup_predio
                  FROM
                    ' || (tmp->>'lyr_over')::text || '
                  GROUP BY id_a                
                ) b
                WHERE
                b.id_a = a.sicob_id
            ';
            RAISE DEBUG 'Running %', sql;
            EXECUTE sql;            
        END IF;
    END IF;
END IF;
    
    --AGREGANDO LA GEOINFORMACION
	PERFORM sicob_add_geoinfo_column(a);
    PERFORM sicob_update_geoinfo_column(a);

	--Eliminando poligonos con superfice menor a la mínima.
    _min_sup := COALESCE((_opt->>'min_sup')::real, 0.002); --> La superficie 0.002 es la menor existente en la BD.
    IF _min_sup > 0 THEN
    	sql := 'DELETE FROM ' || a || 
        ' WHERE sicob_sup < ' || _min_sup::text;
        RAISE DEBUG 'Running %', sql;
        EXECUTE sql;
        GET DIAGNOSTICS _row_cnt = ROW_COUNT;
        IF _row_cnt > 0 THEN
        --RENUMERANDO EL INDICE sicob_id
        	sql := '
            	select CAST(row_number() OVER () AS integer) AS new_sicob_id, t.sicob_id
                from (
                	select * from ' || a || '
                    order by sicob_id
                ) t
            ';
            EXECUTE 'CREATE TEMPORARY TABLE ' || tbl_name || '_newid' || ' ON COMMIT DROP AS ' || sql;
            
        	sql := '
            	UPDATE ' || a || ' a
            	SET sicob_id = (
                	SELECT new_sicob_id
                    FROM ' || tbl_name || '_newid
                    WHERE sicob_id = a.sicob_id
            	)
            ';
            RAISE DEBUG 'Running %', sql;
            EXECUTE sql;
        END IF;
    END IF;    
    
    -->OBTENIENDO LA CANTIDAD DE POLIGONOS ENCONTRADOS.
    EXECUTE 'SELECT count(*) as cnt FROM ' || a || ' WHERE predio IS NOT NULL' INTO inters_cnt;
    -->OBTENIENDO LA CANTIDAD DE POLIGONOS SIN PREDIO ENCONTRADO.
    EXECUTE 'SELECT count(*) as cnt FROM ' || a || ' WHERE predio IS NULL' INTO diff_cnt;
    
    _out := _out::jsonb || ('{"features_inters_cnt":"' || inters_cnt::text || 
    '", "features_diff_cnt":"' || diff_cnt::text || 
    '", "lyr_over":"' || a || '"}')::jsonb;

IF inters_cnt > 0 THEN --> Si se han encontrado poligonos.                  
	-->CREANDO EL DETALLE DE LOS PREDIOS ENCONTRADOS
    createdResult := FALSE;
    FOR lyr_predio IN SELECT * FROM json_array_elements(lyrs_predio) LOOP 
    	sql := '
            WITH ref_pred AS (
                SELECT 
                  predio,
                  propietario,         
                  source_predio,
                  id_predio
                FROM
                  ' || a || ' a
                WHERE
                	source_predio = ''' || COALESCE((lyr_predio->'lyr_pred'->>'source')::text, (lyr_predio->'lyr_parc'->>'source')::text) || '''
                GROUP BY
                  predio,
                  propietario,
                  source_predio,
                  id_predio
                order by  predio, propietario
            ),
            pred AS (
                SELECT a.predio, a.propietario, a.source_predio, a.id_predio,' ||
                  	CASE (lyr_predio->>'subfix')::text WHEN '_tit' 
                    	THEN 'b.titulo' 
                        ELSE 'CAST(NULL AS text)' 
                    END || ' as titulo, ' ||
                    CASE (lyr_predio->>'subfix')::text WHEN '_tit' 
                    	THEN 'b.fecha_titulo' 
                        ELSE 'CAST(NULL AS TEXT)' 
                    END || ' AS fecha_titulo, ' || 
                    CASE (lyr_predio->>'subfix')::text WHEN '_tit' 
                    	THEN 'b.tipo_propiedad' 
                        ELSE 'CAST(NULL AS text)' 
                    END || ' AS tipo_propiedad, ' ||
                    CASE (lyr_predio->>'subfix')::text 
                    	WHEN '_ref'	THEN 'b.sicob_sup'
                        WHEN '_tit'	THEN 'b.sup_predio'
                        WHEN '_pop' THEN 'b.sup_pre' 
                        WHEN '_proc' THEN 'b.sicob_sup' 
                        ELSE 'CAST(NULL AS float)' 
                    END || ' AS sup_predio, ' ||                
                ' st_multi(b.the_geom) as the_geom
                FROM
                	ref_pred a INNER JOIN ' || COALESCE((lyr_predio->'lyr_pred'->>'source')::text, (lyr_predio->'lyr_parc'->>'source')::text) || ' b ON 
                (b.' || COALESCE((lyr_predio->'lyr_pred'->>'fldidpredio')::text, 'sicob_id' ) || ' = a.id_predio)
            )
            SELECT row_number() over() AS sicob_id,* from pred;';
            RAISE DEBUG 'Running %', sql;
        	IF createdResult THEN
				EXECUTE 'INSERT INTO temp.' || tbl_name || '_pred ' || sql;
            ELSE
            	EXECUTE 'DROP TABLE IF EXISTS temp.' || tbl_name || '_pred'; 
                EXECUTE 'CREATE TABLE temp.' || tbl_name || '_pred' || ' AS ' || sql;
                createdResult := TRUE;
        	END IF;
    END LOOP;

	--> AGREGANDO INFORMACION DE TITULACION A LOS PREDIOS CARGADOS POR EL USUARIO
	IF COALESCE( (_opt->>'lyr_parc')::text,'') <> '' THEN --> Si se indica cobertura de referencia.
    	_condition := 'source_predio = ''' || (_opt->>'lyr_parc')::text || '''';
        tmp := sicob_overlap(('{"a":"temp.' || tbl_name || '_pred","condition_a":"' || _condition || '","b":"coberturas.parcelas_tituladas","subfix":"_tit","tolerance":"0","add_diff":false, "temp": true}')::json);
        IF COALESCE( (tmp->>'features_inters_cnt')::int,0) > 0 THEN --> Si se han encontrado parcelas tituladas.
            sql := '
                UPDATE temp.' || tbl_name || '_pred a
                SET titulo = b.titulo, fecha_titulo = b.fecha_titulo, tipo_propiedad = b.tipo_propiedad
                FROM (
                  SELECT id_a, min(idpredio) as idpredio, string_agg(predio , '', '') as predio, string_agg(propietario , '', '') as propietario, string_agg(titulo , '', '') as titulo, string_agg(to_char(fecha_titulo,''DD/MM/YYYY'') , '', '') as fecha_titulo, min(tipo_propiedad) as tipo_propiedad, sum(sup_predio) as sup_predio
                  FROM
                    ' || (tmp->>'lyr_over')::text || '
                  GROUP BY id_a                
                ) b
                WHERE
                b.id_a = a.sicob_id
            ';
            RAISE DEBUG 'Running %', sql;
            EXECUTE sql;            
        END IF;
    END IF;


    -->GENERANDO EL JSON DE INFORMACION DE PREDIOS           
    sql := '
      SELECT row_to_json(u)
      FROM
      (
          SELECT array_to_json(array_agg(t)) as predios, count(t.*) as total_predios FROM 
          (
              SELECT ' || 
              sicob_no_geo_column(
              	'temp.' || tbl_name || '_pred',
                '{}',
                'a.'
              ) || '
              FROM
                temp.' || tbl_name || '_pred a
              ORDER BY a.source_predio, a.predio
          ) t
      )u
    ';   
    RAISE DEBUG 'Running %', sql;
    EXECUTE sql INTO tmp; 
    _out := _out::jsonb || ('{"lyr_pred":"temp.' || tbl_name || '_pred"}')::jsonb || tmp::jsonb;
END IF;

    
    IF COALESCE((_opt->>'geojson')::boolean,FALSE) = TRUE THEN
    	-->ADICIONANDO RESULTADO EN FORMATO GEOJSON
    	EXECUTE 'SELECT sicob_to_geojson(''{"lyr_in":"' ||  a || '"}'') AS geojson' INTO _geojson;
        _out := _out::jsonb || jsonb_build_object('lyr_geojson', _geojson::json); 
    ELSE
    	IF inters_cnt > 0 THEN --> Si se han encontrado poligonos.
     --> ADICIONANDO LA INFORMACION DE LOS POLIGONOS + PREDIOS                   
            sql := '
              SELECT row_to_json(u)
              FROM
              (
                  SELECT array_to_json(array_agg(t)) as poligonos FROM 
                  (
                      SELECT ' || 
                      sicob_no_geo_column(
                        'temp.' || tbl_name || '_ppred',
                        '{}',
                        'a.'
                      ) || '
                      FROM
                        temp.' || tbl_name || '_ppred a
                      ORDER BY a.source_predio, a.predio
                  ) t
              )u
            ';   
            RAISE DEBUG 'Running %', sql;
            EXECUTE sql INTO tmp;
            _out := tmp::jsonb || _out::jsonb;
		END IF;
	END IF;        

RETURN _out;
	
    EXCEPTION
		WHEN others THEN
			RAISE EXCEPTION 'geoSICOB (sicob_obtener_predio): _opt: % >> %, (%)', _opt::text, SQLERRM, sql;

END;
$function$
 