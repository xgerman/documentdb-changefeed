"""Preserve administrator-owned feeds when upgrading the private schema."""

import json

from fixtures import connect, query


def main():
    connection = connect()
    query(connection, "CREATE EXTENSION documentdb_changefeed VERSION '0.1.0'")
    query(connection, "SELECT documentdb_api.create_collection('cf_upgrade','events')")
    query(connection, "SELECT pg_create_logical_replication_slot('cf_upgrade','pg_documentdb_changefeed')")
    query(connection, "SELECT documentdb_changefeed.register_feed('cf_upgrade','events','cf_upgrade')")
    query(connection, "ALTER EXTENSION documentdb_changefeed UPDATE TO '0.2.0'")
    assert query(connection, "SELECT auto_owned,capture_updates FROM documentdb_changefeed.feeds"
                 " WHERE database_name='cf_upgrade'")[0] == (False, False)
    rows = query(connection, "SELECT documentdb_core.bson_to_json_string(continuation)"
                 " FROM documentdb_api_internal.change_stream_aggregation('cf_upgrade','events','{}','{}')")
    assert json.loads(rows[-1][0])["rt"].startswith("cf1:")
    query(connection, "SELECT documentdb_changefeed.unregister_feed('cf_upgrade','events')")
    assert query(connection, "SELECT 1 FROM pg_replication_slots WHERE slot_name='cf_upgrade'")
    query(connection, "SELECT pg_drop_replication_slot('cf_upgrade')")
    connection.close()
    print("PASS schema upgrade: manual feed ownership, original token format and slot lifetime preserved")


if __name__ == "__main__":
    main()
