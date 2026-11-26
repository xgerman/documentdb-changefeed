ALTER TABLE documentdb_changefeed.feeds
    ADD COLUMN auto_owned boolean NOT NULL DEFAULT false,
    ADD COLUMN last_activity timestamptz NOT NULL DEFAULT clock_timestamp(),
    ADD COLUMN restore_update_description boolean NOT NULL DEFAULT false;

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
