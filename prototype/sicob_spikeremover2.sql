CREATE OR REPLACE FUNCTION public.sicob_spikeremover2(geometry, radius double precision)
 RETURNS geometry
 LANGUAGE plpgsql
 IMMUTABLE
AS $function$
declare
	_out geometry;
    exterior_ring geometry;
begin
/*
    SELECT  ST_Collect(ST_ExteriorRing(the_geom))  INTO  exterior_ring
        FROM (
            SELECT (dp).path[1] as gid, (dp).geom as the_geom 
            FROM(
                SELECT st_dump($1) as dp
                ) t        
        ) As u
    GROUP BY gid;
 */ 
     exterior_ring := $1;
    
    IF st_isvalid(exterior_ring) = FALSE THEN
    	RAISE DEBUG 'Es invalida!!! ';
        SELECT st_makevalid(exterior_ring) INTO exterior_ring;
    END IF; 
     
    _out := st_buffer(
                st_buffer(
                    exterior_ring ,
                    -0.0000001,
                    'join=mitre'
                ),
                0.0000001,
                'join=mitre'
            );
      
	_out := st_difference(exterior_ring,_out) ;
  /*        
    SELECT
   st_snap(
        _out,
        exterior_ring,
        0.00000001
    ) INTO _out;  
   */
   


    
	IF geometrytype(_out) = 'GEOMETRYCOLLECTION' THEN
		SELECT ST_CollectionExtract(_out,3) INTO _out;    
        --RAISE NOTICE 'geom: %',st_astext(_out);
    END IF;


    RETURN _out;

	EXCEPTION
		WHEN others THEN
            RAISE EXCEPTION 'geoSICOB-> % (sicob_spikeremover): % (%)',st_astext(_out), SQLERRM, SQLSTATE;	
end;
$function$
 