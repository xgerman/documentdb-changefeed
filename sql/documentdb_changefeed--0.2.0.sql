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
    capture_updates boolean NOT NULL DEFAULT false,
    auto_owned boolean NOT NULL DEFAULT false,
    last_activity timestamptz NOT NULL DEFAULT clock_timestamp(),
    restore_update_description boolean NOT NULL DEFAULT false,
    PRIMARY KEY (database_name, collection_name)
);
REVOKE ALL ON TABLE documentdb_changefeed.feeds FROM PUBLIC;

CREATE TABLE documentdb_changefeed.checkpoints (
    database_name text NOT NULL,
    collection_name text NOT NULL,
    observed_at timestamptz NOT NULL,
    position pg_lsn NOT NULL,
    PRIMARY KEY (database_name, collection_name, observed_at),
    FOREIGN KEY (database_name, collection_name)
        REFERENCES documentdb_changefeed.feeds ON DELETE CASCADE
);
REVOKE ALL ON TABLE documentdb_changefeed.checkpoints FROM PUBLIC;

CREATE TABLE documentdb_changefeed.pending_slots (
    slot text PRIMARY KEY,
    relation_id oid NOT NULL
);
REVOKE ALL ON TABLE documentdb_changefeed.pending_slots FROM PUBLIC;

CREATE FUNCTION documentdb_changefeed.register_feed(text, text, text, boolean DEFAULT false)
RETURNS void
AS 'MODULE_PATHNAME', 'documentdb_changefeed_register'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;
REVOKE ALL ON FUNCTION documentdb_changefeed.register_feed(text, text, text, boolean) FROM PUBLIC;

CREATE FUNCTION documentdb_changefeed.capture_update()
RETURNS trigger
AS 'MODULE_PATHNAME', 'documentdb_changefeed_capture_update'
LANGUAGE C VOLATILE PARALLEL UNSAFE;
REVOKE ALL ON FUNCTION documentdb_changefeed.capture_update() FROM PUBLIC;

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
