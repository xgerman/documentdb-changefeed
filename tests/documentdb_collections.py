import json
import os
import random

from psycopg2 import sql

from fixtures import boundary, connect, decode, expect_error, install_decoder, query, verify


DATABASE = "changefeed_fixture"
COLLECTION = "events"


def insert(connection, identifier, payload):
    query(
        connection,
        "SELECT documentdb_api.insert_one(%s, %s, %s::documentdb_core.bson)",
        (DATABASE, COLLECTION, json.dumps({"_id": identifier, "payload": payload})),
    )


def update(connection, identifier, payload):
    command = {
        "update": COLLECTION,
        "updates": [{"q": {"_id": identifier}, "u": {"$set": {"payload": payload}}}],
    }
    query(
        connection,
        "SELECT documentdb_api.update(%s, %s::documentdb_core.bson)",
        (DATABASE, json.dumps(command)),
    )


def delete(connection, identifier):
    command = {"delete": COLLECTION, "deletes": [{"q": {"_id": identifier}, "limit": 1}]}
    query(
        connection,
        "SELECT documentdb_api.delete(%s, %s::documentdb_core.bson)",
        (DATABASE, json.dumps(command)),
    )


def stored_image(connection, table, identifier):
    canonical = query(
        connection,
        "SELECT documentdb_core.bson_to_json_string(%s::documentdb_core.bson)",
        (json.dumps({"_id": identifier}),),
    )[0][0]
    identifier = json.loads(canonical)["_id"]
    rows = query(
        connection,
        sql.SQL("SELECT documentdb_core.bson_to_json_string(document), documentdb_core.bson_send(document) FROM {}").format(table),
    )
    matches = [
        bytes(raw) for text, raw in rows
        if json.loads(text)["_id"] == identifier
        and type(json.loads(text)["_id"]) is type(identifier)
    ]
    assert len(matches) == 1, "Expected exactly one stored BSON document"
    return matches[0]


def main():
    connection = connect()
    assert int(query(connection, "SHOW server_version_num")[0][0]) // 10000 == int(os.environ["PG_MAJOR"])
    query(connection, "CREATE EXTENSION documentdb CASCADE")
    query(connection, "CREATE EXTENSION documentdb_changefeed")
    assert query(connection, "SELECT documentdb_changefeed.version()")[0][0] == "0.1.0-dev"
    install_decoder(connection)
    query(connection, "SELECT documentdb_api.create_collection(%s, %s)", (DATABASE, COLLECTION))
    collection_id = query(
        connection,
        "SELECT collection_id FROM documentdb_api_catalog.collections WHERE database_name=%s AND collection_name=%s",
        (DATABASE, COLLECTION),
    )[0][0]
    table = sql.Identifier("documentdb_data", f"documents_{collection_id}")
    table_name = f"documentdb_data.documents_{collection_id}"
    query(connection, sql.SQL("ALTER TABLE {} REPLICA IDENTITY FULL").format(table))
    query(connection, "CREATE TABLE flush_marker (id bigint GENERATED ALWAYS AS IDENTITY)")
    query(connection, "SELECT pg_create_physical_replication_slot('cf_fixture', true)")
    start = query(connection, "SELECT pg_current_wal_insert_lsn()")[0][0]
    expected = []
    first = connect()
    second = connect()
    first.autocommit = second.autocommit = False
    try:
        insert(first, 1, "earlier write, later commit")
        image_one = stored_image(first, table, 1)
        insert(second, "1", "later write, earlier commit")
        image_string = stored_image(second, table, "1")
        second.commit()
        first.commit()
        expected.extend([("insert", None, image_string), ("insert", None, image_one)])

        insert(first, "rollback", "must not appear")
        first.rollback()
        query(first, "SAVEPOINT keep")
        insert(first, "kept", "subtransaction")
        kept = stored_image(first, table, "kept")
        query(first, "RELEASE SAVEPOINT keep; SAVEPOINT discard")
        insert(first, "discarded", "must not appear")
        query(first, "ROLLBACK TO SAVEPOINT discard")
        first.commit()
        expected.append(("insert", None, kept))

        compressed = "x" * 1024 * 1024
        generator = random.Random(734)
        random_payloads = [generator.randbytes(100000).hex() for _ in range(3)]
        insert(first, "large", compressed)
        previous = stored_image(first, table, "large")
        first.commit()
        expected.append(("insert", None, previous))
        size = query(connection, sql.SQL("SELECT max(pg_column_size(document)) FROM {}").format(table))[0][0]
        assert 2000 < size < len(compressed) // 10, "Expected compressed external BSON"
        for payload in random_payloads:
            update(first, "large", payload)
            after = stored_image(first, table, "large")
            expected.append(("update", previous, after))
            previous = after
        query(first, "SAVEPOINT rollback_large")
        update(first, "large", compressed)
        query(first, "ROLLBACK TO SAVEPOINT rollback_large")
        delete(first, "large")
        first.commit()
        expected.append(("delete", previous, None))

        # Default collection shard keys repeat; they must never be row identities.
        assert query(connection, sql.SQL("SELECT count(DISTINCT shard_key_value) FROM {}").format(table))[0][0] == 1
        end = boundary(connection)
        rows = decode(connection, start, end, table=table_name)
        verify(rows, expected)
        assert rows[0][0] != rows[1][0], "Commit-order fixture did not overlap"
        assert decode(connection, start, end, table=table_name) == rows
        verify(decode(second, start, end, table=table_name), expected)
        second.commit()
        expect_error("54000", lambda: decode(connection, start, end, memory=1, table=table_name))

        query(connection, "CREATE ROLE cf_reader; SET ROLE cf_reader")
        try:
            expect_error("42501", lambda: query(connection, "SELECT documentdb_changefeed.version()"))
        finally:
            query(connection, "RESET ROLE")
        query(connection, "DROP EXTENSION documentdb_changefeed")
        # Removing this foundation must not affect collection reads.
        assert query(connection, "SELECT count(*) FROM documentdb_api.collection(%s, %s)", (DATABASE, COLLECTION))[0][0] == 3
        query(connection, "SELECT pg_drop_replication_slot('cf_fixture')")
        versions = query(connection, "SELECT extname, extversion FROM pg_extension WHERE extname IN ('documentdb', 'documentdb_core')")
        print(f"PASS real DocumentDB collections: {len(expected)} exact BSON events; {versions}; insert/update/delete APIs, colliding shard keys, typed IDs, TOAST, commit ordering, rollback/savepoints, replay, permissions, install/uninstall")
    finally:
        first.close()
        second.close()
        connection.close()


if __name__ == "__main__":
    main()
