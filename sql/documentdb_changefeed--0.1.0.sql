\echo Use "CREATE EXTENSION documentdb_changefeed" to load this file. \quit

CREATE SCHEMA documentdb_changefeed;
REVOKE ALL ON SCHEMA documentdb_changefeed FROM PUBLIC;

CREATE FUNCTION documentdb_changefeed.version()
RETURNS text
AS 'MODULE_PATHNAME', 'documentdb_changefeed_version'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

REVOKE ALL ON FUNCTION documentdb_changefeed.version() FROM PUBLIC;

CREATE TABLE documentdb_changefeed.feeds (
    database_name text NOT NULL,
    collection_name text NOT NULL,
    slot text NOT NULL UNIQUE,
    relation_id oid NOT NULL,
    storage_id oid NOT NULL,
    generation text NOT NULL,
    start_lsn pg_lsn NOT NULL,
    PRIMARY KEY (database_name, collection_name)
);
REVOKE ALL ON TABLE documentdb_changefeed.feeds FROM PUBLIC;

CREATE FUNCTION documentdb_changefeed.register_feed(text, text, text)
RETURNS void
AS 'MODULE_PATHNAME', 'documentdb_changefeed_register'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;
REVOKE ALL ON FUNCTION documentdb_changefeed.register_feed(text, text, text) FROM PUBLIC;

CREATE FUNCTION documentdb_changefeed.unregister_feed(text, text)
RETURNS void
AS 'MODULE_PATHNAME', 'documentdb_changefeed_unregister'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;
REVOKE ALL ON FUNCTION documentdb_changefeed.unregister_feed(text, text) FROM PUBLIC;

CREATE FUNCTION documentdb_api_internal.change_stream_aggregation(
    text, text, documentdb_core.bson, documentdb_core.bson)
RETURNS TABLE(document documentdb_core.bson, continuation documentdb_core.bson)
AS 'MODULE_PATHNAME', 'documentdb_changefeed_aggregate'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE SECURITY DEFINER
SET search_path = pg_catalog;
