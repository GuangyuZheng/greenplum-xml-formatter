DROP FUNCTION IF EXISTS xml_formatter_import();
CREATE FUNCTION xml_formatter_import() RETURNS record
as '$libdir/xml_formatter.so', 'xml_formatter_import'
LANGUAGE C STABLE;

DROP FUNCTION IF EXISTS xml_formatter_export(record);
CREATE FUNCTION xml_formatter_export(record) RETURNS bytea
as '$libdir/xml_formatter.so', 'xml_formatter_export'
LANGUAGE C STABLE;
