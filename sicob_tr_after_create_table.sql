CREATE EVENT TRIGGER sicob_tr_after_create_table
ON ddl_command_end
WHEN tag IN ('CREATE TABLE', 'ALTER TABLE')
EXECUTE PROCEDURE public.sicob_after_create_table();
