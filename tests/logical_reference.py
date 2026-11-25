"""Exercise the serving provider against real collections and retained WAL."""

import json
import random
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

from fixtures import connect, expect_error, query


DATABASE = "cf_reference"
COLLECTION = "events"


def insert(connection, identifier, payload="value"):
    query(
        connection,
        "SELECT documentdb_api.insert_one(%s,%s,%s::documentdb_core.bson)",
        (DATABASE, COLLECTION, json.dumps({"_id": identifier, "payload": payload})),
    )


def page(connection, token=None, count=100, size=4 * 1024 * 1024, options=None):
    cursor = {"getpage_batchCount": count, "getpage_batchSize": size}
    if token is not None:
        cursor["rt"] = token
    rows = query(
        connection,
        "SELECT documentdb_core.bson_to_json_string(document),"
        " documentdb_core.bson_to_json_string(continuation)"
        " FROM documentdb_api_internal.change_stream_aggregation"
        " (%s,%s,%s::documentdb_core.bson,%s::documentdb_core.bson)",
        (DATABASE, COLLECTION, json.dumps(options or {}), json.dumps(cursor)),
    )
    events = [json.loads(document) for document, _ in rows if document is not None]
    return events, json.loads(rows[-1][1])["rt"]


def main():
    connection = connect()
    query(connection, "CREATE EXTENSION IF NOT EXISTS documentdb CASCADE")
    query(connection, "CREATE EXTENSION IF NOT EXISTS documentdb_changefeed")
    query(connection, "SELECT documentdb_api.create_collection(%s,%s)", (DATABASE, COLLECTION))
    query(connection, "SELECT pg_create_logical_replication_slot('cf_reference','pg_documentdb_changefeed')")
    query(connection, "SELECT documentdb_changefeed.register_feed(%s,%s,'cf_reference')", (DATABASE, COLLECTION))
    relation = query(connection, "SELECT relation_id::regclass::text FROM documentdb_changefeed.feeds")[0][0]
    assert query(connection, "SELECT relreplident FROM pg_class WHERE oid=%s::regclass", (relation,))[0][0] == "d"
    first = connect()
    second = connect()
    first.autocommit = second.autocommit = False
    try:
        insert(connection, "before-watch")
        insert(first, "open-before-watch")
        events, start = page(connection, count=0)
        assert not events and start.startswith("cf1:")
        first.commit()
        insert(first, 1, "later commit")
        insert(second, "1", "earlier commit")
        second.commit()
        first.commit()
        insert(first, "aborted")
        first.rollback()
        query(first, "SAVEPOINT discarded")
        insert(first, "savepoint-aborted")
        query(first, "ROLLBACK TO SAVEPOINT discarded")
        insert(first, "kept")
        first.commit()
        query(first, "SAVEPOINT released")
        insert(first, "parent-aborted")
        query(first, "RELEASE SAVEPOINT released")
        first.rollback()
        compressed = "x" * 1024 * 1024
        incompressible = random.Random(47).randbytes(100000).hex()
        insert(first, "compressed", compressed)
        insert(first, "incompressible", incompressible)
        first.commit()
        query(connection, "SELECT documentdb_api.delete(%s,%s::documentdb_core.bson)",
              (DATABASE, json.dumps({"delete": COLLECTION, "deletes": [{"q": {"_id": 1}, "limit": 1}]})))

        received = []
        continuation = start
        for _ in range(10):
            events, continuation = page(connection, continuation, count=1)
            received.extend(events)
            if not events:
                break
        keys = [event["documentKey"]["_id"] for event in received]
        assert keys == [
            "open-before-watch", "1", {"$numberInt": "1"}, "kept", "compressed",
            "incompressible", {"$numberInt": "1"}
        ], keys
        assert received[-1]["operationType"] == "delete"
        assert "fullDocument" not in received[-1]
        assert received[4]["fullDocument"]["payload"] == compressed
        assert received[5]["fullDocument"]["payload"] == incompressible
        replay, _ = page(connection, start)
        assert replay == received
        readers = Barrier(2)
        def concurrent_read():
            reader = connect()
            try:
                readers.wait(timeout=10)
                return page(reader, start)[0]
            finally:
                reader.close()
        with ThreadPoolExecutor(max_workers=2) as pool:
            concurrent = list(pool.map(lambda _: concurrent_read(), range(2)))
        assert concurrent == [received, received]
        encoded = received[0]["_id"]["_data"]
        resumed, _ = page(connection, options={"resumeAfter": {"_data": encoded}})
        assert resumed == received[1:]
        empty, same = page(connection, start, count=0)
        assert empty == [] and same == start
        expect_error("54000", lambda: page(connection, start, size=1))
        expect_error("22023", lambda: page(connection, start.replace("cf1:", "cf9:")))
        query(connection, "SELECT documentdb_api.create_collection(%s,'other')", (DATABASE,))
        query(connection, "SELECT pg_create_logical_replication_slot('cf_other','pg_documentdb_changefeed')")
        query(connection, "SELECT documentdb_changefeed.register_feed(%s,'other','cf_other')", (DATABASE,))
        other = query(connection, "SELECT documentdb_core.bson_to_json_string(continuation)"
                      " FROM documentdb_api_internal.change_stream_aggregation(%s,'other','{}','{}')",
                      (DATABASE,))[0][0]
        expect_error("22023", lambda: page(connection, json.loads(other)["rt"]))
        query(connection, "SELECT documentdb_changefeed.unregister_feed(%s,'other')", (DATABASE,))
        query(connection, "SELECT pg_drop_replication_slot('cf_other')")
        expect_error("0A000", lambda: page(connection, options={"startAtOperationTime": 0}))
        query(connection, "SET documentdb_changefeed.enable=off")
        expect_error("55000", lambda: page(connection))
        query(connection, "RESET documentdb_changefeed.enable")
        query(connection, "CREATE ROLE cf_reference_reader")
        query(connection, "GRANT USAGE ON SCHEMA documentdb_core, documentdb_api_internal TO cf_reference_reader")
        query(connection, "SET ROLE cf_reference_reader")
        expect_error("42501", lambda: page(connection, start))
        query(connection, "RESET ROLE")
        query(connection, f"GRANT SELECT ON {relation} TO cf_reference_reader")
        query(connection, "SET ROLE cf_reference_reader")
        authorized, _ = page(connection, start)
        assert authorized == received
        query(connection, "RESET ROLE")

        # Updates are fail-stop; an older unsupported mutation must not break a new subscription.
        query(connection, "SELECT documentdb_api.update(%s,%s::documentdb_core.bson)",
              (DATABASE, json.dumps({"update": COLLECTION, "updates": [
                  {"q": {"_id": "kept"}, "u": {"$set": {"payload": "changed"}}}]})))
        expect_error("0A000", lambda: page(connection, continuation))
        _, after_update = page(connection)
        insert(connection, "after-update")
        after, _ = page(connection, after_update)
        assert len(after) == 1 and after[0]["documentKey"]["_id"] == "after-update"
        query(connection, "SELECT documentdb_changefeed.unregister_feed(%s,%s)", (DATABASE, COLLECTION))
        assert query(connection, "SELECT count(*) FROM pg_replication_slots WHERE slot_name='cf_reference'")[0][0] == 1
        query(connection, "SELECT pg_drop_replication_slot('cf_reference')")
        assert query(connection, "SELECT count(*) FROM documentdb_api.collection(%s,%s)", (DATABASE, COLLECTION))[0][0] == 7
        print("PASS logical reference: real collections, default identity, exact insert/delete images, "
              "typed keys, commit order, abort/savepoints, TOAST, paging/resume, replay, "
              "zero-size batches, limits, authorization, disablement, unsupported updates and cleanup")
    finally:
        first.close()
        second.close()
        connection.close()


if __name__ == "__main__":
    main()
