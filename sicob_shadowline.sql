SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_shadowline(_edge geometry, _line geometry)
 RETURNS geometry
 LANGUAGE plpgsql
AS $function$
DECLARE
 new_line geometry;
 epos int;
 pnt geometry;
 connline geometry;
 startpoint geometry;
 endpoint geometry;
 refpoint geometry;
 
BEGIN

/*
_edge := 'LINESTRING(-66.008587481079 -15.2726145178684,-66.011238612074 -15.2736784441754,-66.0113833889999 -15.2736564429999,-66.0114796767628 -15.2736733386568,-66.0125053368261 -15.2740805204448,-66.012462152 -15.273902527,-66.0124539079999 -15.2738771169999,-66.012414757 -15.2738183399999,-66.012035985 -15.2734383979999,-66.01203578 -15.273438261,-66.0120356419999 -15.273438056,-66.011975849 -15.2733981069999,-66.011935394 -15.273386274,-66.011418707 -15.273295602,-66.011388171 -15.2732927389999,-66.0113568369999 -15.273294896,-66.0105778656674 -15.2734132798038,-66.0097857455887 -15.2730953938179,-66.0096608109999 -15.272939493,-66.009647318 -15.2729244499999,-66.0096263579999 -15.272906463,-66.009220534 -15.2726078269999,-66.009201807 -15.272597236,-66.00919361 -15.272591333,-66.009065033 -15.2725267819999,-66.009059177 -15.2725253579999,-66.0090531239999 -15.272521313,-66.008982133 -15.272506617,-66.008766354 -15.2725037339999,-66.008694974 -15.2725165279999,-66.008634071 -15.272554865,-66.008610453 -15.2725820549999,-66.008587481079 -15.2726145178684)'::geometry;
_line := 'LINESTRING(-66.00858745447 -15.2726145071899,-66.008610422433 -15.2725820503213,-66.0086340404286 -15.2725548602186,-66.0086949416182 -15.2725165230191,-66.0087663226076 -15.2725037292636,-66.0089821023421 -15.2725066115908,-66.0090530921265 -15.2725213083117,-66.0090591463625 -15.2725253525629,-66.009065000949 -15.2725267770891,-66.0091935779212 -15.2725913277275,-66.009201776141 -15.2725972308775,-66.0092205018246 -15.2726078221932,-66.0096263271932 -15.2729064582664,-66.0096472858935 -15.2729244447073,-66.0096607802208 -15.2729394876672,-66.0098453338381 -15.2731697856268)'::geometry;
*/

	epos := ST_NPoints(_line);
    pnt := st_pointn(_line,epos);
    connline := ST_MakeLine(ARRAY[pnt, ST_ClosestPoint(_edge, pnt) ]);
    WHILE epos > 2 and st_crosses(connline, _line) LOOP
    	epos := epos - 1;
        pnt := st_pointn(_line,epos);
        connline := ST_MakeLine(ARRAY[pnt, ST_ClosestPoint(_edge, pnt) ]);
	END LOOP;
    IF epos <> ST_NPoints(_line) THEN
    	new_line := ST_Reverse ( ST_LineSubstring(_line, 0, ST_LineLocatePoint(_line, pnt) ) );
    ELSE
    	new_line := ST_Reverse( _line);
    END IF;
    
	epos := ST_NPoints(new_line);
    pnt := st_pointn(new_line,epos);
    connline := ST_MakeLine(ARRAY[pnt, ST_ClosestPoint(_edge, pnt) ]);
    WHILE epos > 2 and st_crosses(connline, new_line) LOOP
    	epos := epos - 1;
        pnt := st_pointn(new_line,epos);
        connline := ST_MakeLine(ARRAY[pnt, ST_ClosestPoint(_edge, pnt) ]);
	END LOOP;
    IF epos <> ST_NPoints(new_line) THEN
    	new_line :=  ST_LineSubstring(new_line, 0, ST_LineLocatePoint(new_line, pnt) ) ;
    END IF;

startpoint := ST_ClosestPoint(_edge, st_startpoint(new_line));
endpoint := ST_ClosestPoint(_edge, st_endpoint(new_line));

IF ST_GeometryType(new_line) = 'ST_Point' THEN
	refpoint := new_line;
ELSE
	refpoint := ST_ClosestPoint(_edge, st_lineinterpolatepoint(new_line, 0.5));
END IF;

new_line := sicob_st_linesubstring(
_edge,
startpoint,
endpoint,
refpoint
);

RETURN new_line;

EXCEPTION
WHEN others THEN
            RAISE EXCEPTION 'geoSICOB (sicob_shadowline): _edge: % | _line: %| new_line: % | %,  (%)', st_astext(_edge),st_astext(_line), st_astext(new_line), SQLERRM, SQLSTATE;	
END;
$function$
 