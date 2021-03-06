SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_fix_line_overlap(_line geometry)
 RETURNS geometry
 LANGUAGE plpgsql
AS $function$
declare
  _i integer;
  _p geometry[];
  _path geometry;
begin

/*
 _line = st_geomfromtext('LINESTRING(-65.9878760180081 -15.2643526851405,-65.987778582867 -15.2643145293394,-65.9877827521718 -15.2643161620477,-65.9877865930681 -15.2643176661506)',4326) ;
 
 */
 /*
 _line = st_geomfromtext('LINESTRING(-65.9872623697782 -15.2618781231419,-65.987258756 -15.261882331,-66.0488389618594 -15.190178768873)');
 */
 
  -- Check geometry type
  if st_geometrytype(_line)<>'ST_LineString'  then --or st_npoints(_line) is null
    return null;
  end if;

  -- Decompose linestring into matrix of points, but checking no same point in sequence
  _p = array[st_pointn(_line, 1)];

  for _i in 2..st_npoints(_line) loop
    if not st_equals(_p[array_length(_p, 1)], st_pointn(_line, _i)) then  
      _p = _p || st_pointn(_line, _i);
    end if;
  end loop;

  _i = 3;
  
  if array_length(_p,1) >= 2 THEN
  	_path = st_makeline(_p[1],_p[2]);
  end if;

  while _i<=array_length(_p,1) loop
    RAISE DEBUG 'path: %; pto: %', st_astext(_path), st_astext(st_buffer( _p[_i], 0.0000001 ));
    IF ST_Intersects (  st_buffer( _p[_i], 0.0000001 ),_path) THEN
    	--si se solapa el punto a la linea
        _p = gs__pullfromarray(_p, _i);--eliminando el punto
    else
    	_path = st_makeline(_path,_p[_i]);
        _i = _i + 1;
    END IF;  
  
    RAISE DEBUG 'path: %', st_astext(_path);
  end loop;

  return _path;
  
	EXCEPTION
	WHEN others THEN
		RAISE EXCEPTION 'sicob_fix_line_overlap: (%, %)', SQLERRM, SQLSTATE;	  
  
end
$function$
 