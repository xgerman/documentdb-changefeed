\echo Use "CREATE EXTENSION documentdb_changefeed" to load this file. \quit

CREATE SCHEMA documentdb_changefeed;
REVOKE ALL ON SCHEMA documentdb_changefeed FROM PUBLIC;

CREATE FUNCTION documentdb_changefeed.version()
RETURNS text
AS 'MODULE_PATHNAME', 'documentdb_changefeed_version'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

REVOKE ALL ON FUNCTION documentdb_changefeed.version() FROM PUBLIC;
