CREATE FUNCTION clone(text) RETURNS int8
    AS  'MODULE_PATHNAME','clone' 
    LANGUAGE C STRICT;

