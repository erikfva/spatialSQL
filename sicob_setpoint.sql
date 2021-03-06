SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_setpoint(_line geometry, _pos integer, _topnt geometry, _lineref geometry)
 RETURNS geometry
 LANGUAGE plpgsql
 IMMUTABLE STRICT COST 1
AS $function$
DECLARE 
  _i integer;
  _p geometry[];
  _crosspoint geometry;
  new_line geometry;
BEGIN
/*
_line:= st_geomfromtext('
LINESTRING(-65.9829292803582 -14.9999840553936,-65.982929299 -14.99998406,-65.960443313 -14.9350309239999,-65.9604432927493 -14.93503098373)
');
_pos := 0;
_topnt := st_geomfromtext('
POINT(-65.9829830536709 -14.999988553751)
');
_lineref := st_geomfromtext('
LINESTRING(-65.9829830536709 -14.999988553751,-65.941862219 -14.9898364549999,-65.9604596276972 -14.9349795049938)
');
*/
/*
_line:= st_geomfromtext('
LINESTRING(-65.9648984140729 -14.9217572732296,-65.9649618024597 -14.9216984544292,-65.9649618030469 -14.9216984526967,-65.9653209118148 -14.9204928090227,-65.965316221 -14.920495941,-65.965167793 -14.920718956,-65.9651065469999 -14.92093861,-65.965054516 -14.9210975299999,-65.9649961329999 -14.9212422909999,-65.96492101 -14.9212689029999,-65.9647357979999 -14.921408888,-65.9645413969999 -14.921589287,-65.964385821 -14.921758418,-65.964268541 -14.9219007639999,-65.964159953 -14.9220593059999,-65.964048707 -14.9223030969999,-65.9639991129999 -14.92249279,-65.963972874 -14.922696565,-65.9639665889999 -14.922897887,-65.963971221 -14.923107656,-65.963976089 -14.923325179,-65.963997912 -14.923513892,-65.964040601 -14.9236333649999,-65.964128462 -14.9237966759999,-65.9642106980104 -14.9239145235818,-65.9643682340529 -14.923449730679,-65.9643682344582 -14.9234497294831,-65.964362421 -14.9234334519999,-65.9643539416504 -14.923360112909) | _pos:0 | _topnt:POINT(-65.9649302055817 -14.9217916816132)
');
_pos := 0;
_lineref := st_geomfromtext('
LINESTRING(-65.9649302055817 -14.9217916816132,-65.964961804 -14.921698453,-65.9648084789999 -14.921840725,-65.96466943 -14.921991905,-65.9645686659999 -14.9221142059999,-65.964486665 -14.9222339149999,-65.9644009909999 -14.922421673,-65.964364828 -14.9225599869999,-65.964343643 -14.9227245389999,-65.964338185 -14.922899499,-65.9643425949999 -14.9230998429999,-65.9643471099999 -14.9233010249999,-65.964362421 -14.9234334519999,-65.9643682349999 -14.9234497309999,-65.9644004669995 -14.9233546330556)
'); 
*/
  -- Check geometry type
  if st_geometrytype(_line)<>'ST_LineString' OR st_geometrytype(_lineref)<>'ST_LineString' then
    return null;
  end if;
  
  if st_crosses(_line, _lineref) THEN
	--_crosspoint := ST_Intersection(_line,_lineref);
    SELECT (dpt).geom FROM
    	(SELECT st_dump(ST_Intersection(_line,_lineref) ) as dpt) t
    ORDER BY st_distance((dpt).geom,_topnt) limit 1 INTO _crosspoint;
    
    
    
    if ST_Length(
    	ST_LineSubstring(
        	_line, 
            ST_LineLocatePoint(_line, st_startpoint(_line)), 
            ST_LineLocatePoint(_line, _crosspoint)
        )
       ) 
       > 
		ST_Length(
        	ST_LineSubstring(
            	_line, 
                ST_LineLocatePoint(_line,_crosspoint), 
                ST_LineLocatePoint(_line,st_endpoint(_line))
            )
       )
    THEN
    	_line := ST_LineSubstring(
        	_line, 
            ST_LineLocatePoint(_line, st_startpoint(_line)), 
            ST_LineLocatePoint(_line, _crosspoint)
        );
    ELSE
    	_line := ST_LineSubstring(
            	_line, 
                ST_LineLocatePoint(_line,_crosspoint), 
                ST_LineLocatePoint(_line,st_endpoint(_line))
         );
    END IF;

  end if;
  
    IF _pos > st_npoints(_line)-1 THEN
    	_pos := st_npoints(_line)-1;
    END IF;
       
	new_line := st_setpoint(_line,_pos,_topnt);
    
    --return new_line;
    
    --verificar que new_line no se cruce con _lineref
    if st_crosses(new_line, _lineref) THEN
		if(st_equals(_topnt, st_endpoint(_lineref)) ) then
			_lineref := st_reverse(_lineref);
		end if;

		-- Decompose linestring into matrix of points, but checking no same point in sequence
		_p := array[st_pointn(_lineref, 1)];

		for _i in 2..st_npoints(_lineref) loop
			if not st_equals(_p[array_length(_p, 1)], st_pointn(_lineref, _i)) then  
				_p := _p || st_pointn(_lineref, _i);
			end if;
		end loop;
            
        _i := 2; -- Ya se movio anteriorrmete el vertice _pos al punto 1 ahora toca moverlo al punto 2
		while _i<=array_length(_p,1) and st_crosses(new_line, _lineref) loop
        	RAISE DEBUG 'new_ine %', st_astext(new_line);
        	new_line := st_setpoint(new_line,_pos,_p[_i]);
            RAISE DEBUG 'new_ine %', st_astext(new_line);
        	_i := _i + 1;
        end loop;
        	
    end if;
    
    RAISE DEBUG 'new_line %', st_astext(new_line);
    
    return new_line;
    
	EXCEPTION
	WHEN others THEN
		RAISE EXCEPTION 'sicob_setpoint: (%, %) | _line:% | _pos:% | _topnt:% | _lineref:%', SQLERRM, SQLSTATE, st_astext(_line), _pos, st_astext(_topnt), st_astext(_lineref);	
END;
$function$
 