CREATE OR REPLACE FUNCTION check_char_valid(val varchar)
	RETURNS boolean
	LANGUAGE plpgsql
	AS $$ 
DECLARE
BEGIN
	if val != '' then
		if position('未知' in val) > 0 then
			return FALSE;
		end if;
		
		if position('999' in val) > 0 then
			return FALSE;
		end if;
	end if;
	
	return TRUE;
END;
$$;