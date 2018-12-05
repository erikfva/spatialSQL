SET CLIENT_ENCODING TO 'utf8';
CREATE OR REPLACE FUNCTION public.sicob_create_id_column(reloid regclass)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
/*
DECLARE
  sql TEXT;
  rec RECORD;
  rec2 RECORD;
  had_column BOOLEAN;
  i INTEGER;
  new_name TEXT;
  sicob_id_name TEXT;*/
BEGIN
--ADICIONA A LA TABLA UN CAMPO DE IDENTIFICACION UNICO LLAMADO "sicob_id"
--------------------------------------------------------------------------

	PERFORM sicob_create_id_column(reloid,'sicob_id');

/* 
 << sicob_id_setup >>
  LOOP --{
    had_column := FALSE;
    BEGIN
      sql := Format('ALTER TABLE %s ADD sicob_id SERIAL NOT NULL UNIQUE', reloid::text);
      RAISE DEBUG 'Running %', sql;
      EXECUTE sql;
      sicob_id_name := 'sicob_id';
      EXIT sicob_id_setup;
      EXCEPTION
      WHEN duplicate_column THEN
        RAISE NOTICE 'Column sicob_id already exists';
        had_column := TRUE;
      WHEN others THEN
        RAISE EXCEPTION 'sicobfying % (sicob_id): % (%)', reloid, SQLERRM, SQLSTATE;
    END;

    IF had_column THEN
      SELECT pg_catalog.pg_get_serial_sequence(reloid::text, 'sicob_id')
        AS seq INTO rec2;

      -- Check data type is an integer
      SELECT
        pg_catalog.pg_get_serial_sequence(reloid::text, 'sicob_id') as seq,
        t.typname, t.oid, a.attnotnull FROM pg_type t, pg_attribute a
      WHERE a.atttypid = t.oid AND a.attrelid = reloid AND NOT a.attisdropped AND a.attname = 'sicob_id'
      INTO STRICT rec;

      -- 20=int2, 21=int4, 23=int8
      IF rec.oid NOT IN (20,21,23) THEN -- {
        RAISE NOTICE 'Existing sicob_id field is of invalid type % (need int2, int4 or int8), renaming', rec.typname;
      ELSIF rec.seq IS NULL THEN -- }{
        RAISE NOTICE 'Existing sicob_id field does not have an associated sequence, renaming';
      ELSE -- }{
        sql := Format('ALTER TABLE %s ALTER COLUMN sicob_id SET NOT NULL', reloid::text);
        IF NOT EXISTS ( SELECT c.conname FROM pg_constraint c, pg_attribute a
        WHERE c.conkey = ARRAY[a.attnum] AND c.conrelid = reloid
              AND a.attrelid = reloid
              AND NOT a.attisdropped
              AND a.attname = 'sicob_id'
              AND c.contype IN ( 'u', 'p' ) ) -- unique or pkey
        THEN
          sql := sql || ', ADD unique(sicob_id)';
        END IF;
        BEGIN
          RAISE DEBUG 'Running %', sql;
          EXECUTE sql;
          sicob_id_name := 'sicob_id';
          EXIT sicob_id_setup;
          EXCEPTION
          WHEN unique_violation OR not_null_violation THEN
            RAISE NOTICE '%, renaming', SQLERRM;
          WHEN others THEN
            RAISE EXCEPTION 'sicobfying % (sicob_id): % (%)', reloid, SQLERRM, SQLSTATE;
        END;
      END IF; -- }

      -- invalid column, need rename and re-create it
      i := 0;
      << rename_column >>
      BEGIN
      sql := Format('ALTER TABLE %s DROP COLUMN sicob_id', reloid::text);
      RAISE DEBUG 'Running %', sql;
      EXECUTE sql;
      EXCEPTION
      WHEN others THEN
        RAISE NOTICE 'Could not rename/delete sicob_id with existing values: % (%)',
        SQLERRM, SQLSTATE;      
      CONTINUE sicob_id_setup;      
      END;

    END IF;
  END LOOP; -- }

  -- Try to copy data from new name if possible
  IF new_name IS NOT NULL THEN
    RAISE NOTICE 'Trying to recover data from % column', new_name;
    BEGIN
      -- Copy existing values to new field
      -- NOTE: using ALTER is a workaround to a PostgreSQL bug and is also known to be faster for tables with many rows
      -- See http://www.postgresql.org/message-id/20140530143150.GA11051@localhost
      sql := Format('ALTER TABLE %s ALTER sicob_id TYPE int USING %I::integer', reloid::text, new_name);
      RAISE DEBUG 'Running %', sql;
      EXECUTE sql;

      -- Find max value
      sql := Format('SELECT coalesce(max(sicob_id), 0) as max FROM %s', reloid::text);
      RAISE DEBUG 'Running %', sql;
      EXECUTE sql INTO rec;

      -- Find sequence name
      SELECT pg_catalog.pg_get_serial_sequence(reloid::text, 'sicob_id')
        AS seq INTO rec2;

      -- Reset sequence name
      sql := Format('ALTER SEQUENCE %s RESTART WITH %s', rec2.seq::text, rec.max + 1);
      RAISE DEBUG 'Running %', sql;
      EXECUTE sql;

      -- Drop old column (all went fine if we got here)
      sql := Format('ALTER TABLE %s DROP %I', reloid::text, new_name);
      RAISE DEBUG 'Running %', sql;
      EXECUTE sql;

      EXCEPTION
      WHEN others THEN
        RAISE NOTICE 'Could not initialize sicob_id with existing values: % (%)',
        SQLERRM, SQLSTATE;
    END;
  END IF;

  -- Set primary key of the table if not already present (e.g. tables created from SQL API)
  IF sicob_id_name IS NULL THEN
    RAISE EXCEPTION 'sicobfying % (Didnt get sicob_id field name)', reloid;
  END IF;
  BEGIN

    -- Is there already a primary key on this table for 
    -- a column other than our chosen primary key?
    SELECT ci.relname AS pkey
    INTO rec
    FROM pg_class c 
    JOIN pg_attribute a ON a.attrelid = c.oid 
    LEFT JOIN pg_index idx ON c.oid = idx.indexrelid AND a.attnum = ANY(idx.indkey)
    JOIN pg_class ci ON idx.indexrelid = ci.oid
    WHERE c.oid = reloid::regclass 
    AND NOT a.attisdropped
    AND a.attname != 'sicob_id'
    AND idx.indisprimary;

    -- Yes? Then drop it, we're adding our own PK to the column
    -- we prefer.
    IF FOUND THEN
      RAISE DEBUG 'sicob_create_id_column: dropping unwanted primary key ''%''', rec.pkey;
      sql := Format('ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s', reloid::text, rec.pkey);
      EXECUTE sql;
    END IF;  

    sql := Format('ALTER TABLE %s ADD PRIMARY KEY (sicob_id)', reloid::text);
    EXECUTE sql;
    EXCEPTION
    WHEN others THEN
      RAISE DEBUG 'Table % CanÂ´t add PRIMARY KEY', reloid;
  END;
*/
END;
$function$
;CREATE OR REPLACE FUNCTION public.sicob_create_id_column(reloid regclass, _sicob_id_name text)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
  sql TEXT;
  rec RECORD;
  rec2 RECORD;
  had_column BOOLEAN;
  i INTEGER;
  new_name TEXT;
  sicob_id_name TEXT;
BEGIN

--	reloid := 'temp._test_muni';

--ADICIONA A LA TABLA UN CAMPO DE IDENTIFICACION UNICO LLAMADO "sicob_id"
--------------------------------------------------------------------------
      IF _sicob_id_name IS NOT NULL AND _sicob_id_name <> '' THEN
  		sicob_id_name := _sicob_id_name;
	  ELSE 
        sicob_id_name := 'sicob_id';
      END IF;
  << sicob_id_setup >>
  LOOP --{
    had_column := FALSE;
    BEGIN     
      sql := Format('ALTER TABLE %s ADD %s SERIAL NOT NULL UNIQUE', reloid::text, sicob_id_name);
      RAISE DEBUG 'Running %', sql;
      EXECUTE sql;
         
      EXIT sicob_id_setup;
      EXCEPTION
      WHEN duplicate_column THEN
        RAISE NOTICE 'Column sicob_id already exists';
        had_column := TRUE;
      WHEN others THEN
      	IF SQLSTATE = 42701 THEN
        	RAISE NOTICE 'Column sicob_id already exists';
        	had_column := TRUE;
        ELSE
        	RAISE EXCEPTION 'sicobfying % (sicob_create_id_column): % (%)', reloid, SQLERRM, SQLSTATE;
		END IF;
   END;

    IF had_column THEN
      SELECT pg_catalog.pg_get_serial_sequence(reloid::text, sicob_id_name)
        AS seq INTO rec2;

      -- Check data type is an integer
      SELECT
        pg_catalog.pg_get_serial_sequence(reloid::text, sicob_id_name) as seq,
        t.typname, t.oid, a.attnotnull FROM pg_type t, pg_attribute a
      WHERE a.atttypid = t.oid AND a.attrelid = reloid::regclass AND NOT a.attisdropped AND a.attname = sicob_id_name
      INTO STRICT rec;

      -- 20=int2, 21=int4, 23=int8
      IF rec.oid NOT IN (20,21,23) THEN -- {
        RAISE NOTICE 'Existing % field is of invalid type % (need int2, int4 or int8), renaming', sicob_id_name, rec.typname;
      ELSIF rec.seq IS NULL THEN -- }{
        RAISE NOTICE 'Existing sicob_id field does not have an associated sequence, renaming';
      ELSE -- }{
        sql := Format('ALTER TABLE %s ALTER COLUMN %s SET NOT NULL', reloid::text, sicob_id_name);
        IF NOT EXISTS ( SELECT c.conname FROM pg_constraint c, pg_attribute a
        WHERE c.conkey = ARRAY[a.attnum] AND c.conrelid = reloid
              AND a.attrelid = reloid
              AND NOT a.attisdropped
              AND a.attname = sicob_id_name
              AND c.contype IN ( 'u', 'p' ) ) -- unique or pkey
        THEN
          sql := sql || ', ADD unique(' || sicob_id_name || ')';
        END IF;
        BEGIN
          RAISE DEBUG 'Running %', sql;
          EXECUTE sql;
          --sicob_id_name := 'sicob_id';
          EXIT sicob_id_setup;
          EXCEPTION
          WHEN unique_violation OR not_null_violation THEN
            RAISE NOTICE '%, renaming', SQLERRM;
          WHEN others THEN
            RAISE EXCEPTION 'sicobfying % (sicob_id): % (%)', reloid, SQLERRM, SQLSTATE;
        END;
      END IF; -- }

      -- invalid column, need rename and re-create it
      i := 0;
      << rename_column >>
      BEGIN
      sql := Format('ALTER TABLE %s DROP COLUMN %s', reloid::text,sicob_id_name);
      RAISE DEBUG 'Running %', sql;
      EXECUTE sql;
      EXCEPTION
      WHEN others THEN
        RAISE NOTICE 'Could not rename/delete sicob_id with existing values: % (%)',
        SQLERRM, SQLSTATE;      
      CONTINUE sicob_id_setup;      
      END;

    END IF;
  END LOOP; -- }

  -- Try to copy data from new name if possible
  IF new_name IS NOT NULL THEN
    RAISE NOTICE 'Trying to recover data from % column', new_name;
    BEGIN
      -- Copy existing values to new field
      -- NOTE: using ALTER is a workaround to a PostgreSQL bug and is also known to be faster for tables with many rows
      -- See http://www.postgresql.org/message-id/20140530143150.GA11051@localhost
      sql := Format('ALTER TABLE %s ALTER %s TYPE int USING %I::integer', reloid::text,sicob_id_name, new_name);
      RAISE DEBUG 'Running %', sql;
      EXECUTE sql;

      -- Find max value
      sql := Format('SELECT coalesce(max(%s), 0) as max FROM %s',sicob_id_name, reloid::text);
      RAISE DEBUG 'Running %', sql;
      EXECUTE sql INTO rec;

      -- Find sequence name
      SELECT pg_catalog.pg_get_serial_sequence(reloid::text, sicob_id_name)
        AS seq INTO rec2;

      -- Reset sequence name
      sql := Format('ALTER SEQUENCE %s RESTART WITH %s', rec2.seq::text, rec.max + 1);
      RAISE DEBUG 'Running %', sql;
      EXECUTE sql;

      -- Drop old column (all went fine if we got here)
      sql := Format('ALTER TABLE %s DROP %I', reloid::text, new_name);
      RAISE DEBUG 'Running %', sql;
      EXECUTE sql;

      EXCEPTION
      WHEN others THEN
        RAISE NOTICE 'Could not initialize sicob_id with existing values: % (%)',
        SQLERRM, SQLSTATE;
    END;
  END IF;

  -- Set primary key of the table if not already present (e.g. tables created from SQL API)
  IF sicob_id_name IS NULL THEN
    RAISE EXCEPTION 'sicobfying % (Didnt get % field name)', reloid, sicob_id_name;
  END IF;
  BEGIN

    -- Is there already a primary key on this table for 
    -- a column other than our chosen primary key?
    SELECT c.conname as pkey into rec FROM pg_constraint c, pg_attribute a
        WHERE c.conkey = ARRAY[a.attnum] AND c.conrelid = reloid::regclass
              AND a.attrelid = reloid::regclass
              AND NOT a.attisdropped
              AND c.contype IN (  'p' );

    -- Yes? Then drop it, we're adding our own PK to the column
    -- we prefer.
    IF FOUND THEN
      RAISE DEBUG 'sicob_create_id_column: dropping unwanted primary key ''%''', rec.pkey;
      sql := Format('ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s', reloid::text, rec.pkey);
      EXECUTE sql;
    END IF;  

    sql := Format('ALTER TABLE %s ADD PRIMARY KEY (%s)', reloid::text,sicob_id_name);
    EXECUTE sql;
    EXCEPTION
    WHEN others THEN
      RAISE DEBUG 'Table % CanÂ´t add PRIMARY KEY', reloid;
  END;

END;
$function$
 