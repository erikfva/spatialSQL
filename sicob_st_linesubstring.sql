SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_st_linesubstring(line geometry, startpoint geometry, endpoint geometry, refpoint geometry)
 RETURNS geometry
 LANGUAGE plpgsql
AS $function$
DECLARE 
  azimuth_A_B double precision;
  azimuth_B_A double precision;
  d1 double precision;
  d2 double precision;
  subline1 geometry;
  subline2 geometry;
  
BEGIN
/*
	azimuth_A_B := ST_Azimuth(startpoint,endpoint);	
    azimuth_B_A := ST_Azimuth(endpoint, startpoint);	
*/

/*
 select the_geom into line FROM temp._borde limit 1;
 startpoint := st_startpoint(st_geomfromtext('LINESTRING(-65.9399012251724 -14.8652041351882,-65.9398739659107 -14.8652360397114)', 4326 ));
 endpoint := st_endpoint(st_geomfromtext('LINESTRING(-65.9399012251724 -14.8652041351882,-65.9398739659107 -14.8652360397114)',4326 ));
 refpoint := st_lineinterpolatepoint(st_makeline(startpoint,endpoint), 0.5);
 */
 
 /*
 SELECT st_union(the_geom) into line FROM temp._borde;
 SELECT ST_ClosestPoint(line,st_startpoint(the_geom)), st_endpoint(the_geom),st_lineinterpolatepoint(the_geom, 0.5) into startpoint,endpoint,refpoint FROM temp._line_pol_b
 WHERE idpol_b = 7;
*/
/*
line := st_geomfromtext('LINESTRING(-62.4419591643761 -17.6335777505114,-62.441930518342 -17.638976274835,-62.4421415649626 -17.6389944729068,-62.4419591643761 -17.6335777505114)', 4326 );
startpoint := st_geomfromtext('POINT(-62.441959164414 -17.6335777516373)', 4326 );
 endpoint := st_geomfromtext('POINT(-62.441930518344 -17.6389762748352)' , 4326 );
 refpoint := st_geomfromtext('POINT(-62.4419448413561 -17.6362770132361)' , 4326 );
 */

    d1:=ST_LineLocatePoint(line, startpoint);
    d2:=ST_LineLocatePoint(line, endpoint);

    IF d1 < d2  THEN
      subline1 := ST_LineSubstring(line, d1, d2);
    ELSE
      subline1 := ST_LineSubstring( line , d2, d1);
    END IF;
    
    IF (d1*10000)::int = (d2*10000)::int THEN --> La sombra forma un punto.
    	RETURN subline1;
    END IF;
 
    
   RAISE DEBUG 'Running %', st_astext(subline1);

/*
	subline2 := st_linemerge(
					st_difference(
						line,
						st_buffer(
    						subline1,
    						0.0000000001, 
                            'endcap=flat' 
                        )
					)
				);    
 */
 
SELECT 
	the_geom INTO subline2
FROM(
		SELECT(
			st_dump(
				st_linemerge(
					st_difference(
						line,
						st_buffer(
    						subline1,
    						0.0000000001, 
                            'endcap=flat' 
                        )
					)
				)
			)
		).geom as the_geom
) t 
ORDER BY st_length(the_geom) DESC LIMIT 1; 
 
  
    IF ST_Distance(refpoint,subline1) < ST_Distance(refpoint,subline2) THEN
    RAISE DEBUG 'Running %', st_astext(subline1);
    	RETURN subline1;
    ELSE
    RAISE DEBUG 'Running %', st_astext(subline2);
    	--Ajustando los vertices extremos a los originales, movidos por la distancia del buffer aplicado.
        IF ST_Distance(st_startpoint(subline2) ,startpoint) < ST_Distance(st_startpoint(subline2) ,endpoint) THEN
        	subline2 := st_setpoint( st_setpoint(subline2,0,startpoint), ST_NumPoints(subline2) - 1,endpoint);
        ELSE
        	subline2 := st_setpoint( st_setpoint(subline2,0,endpoint),ST_NumPoints(subline2) - 1,startpoint);
        END IF;
    	RETURN subline2;
    END IF;

EXCEPTION
WHEN others THEN
            RAISE EXCEPTION 'geoSICOB (sicob_st_linesubstring): line: % | typegeom: % |startpoint: % | endpoint: % | refpoint: % | d1: % | d2: % | %,(state:%)', st_astext(line), st_geometrytype(line),st_astext(startpoint),st_astext(endpoint), st_astext(refpoint), d1, d2, SQLERRM, SQLSTATE;
            	
END;
$function$
 