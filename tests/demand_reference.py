"""Prove automatic capture, authorization, bounded resume and owned cleanup."""

import json
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

from fixtures import connect, expect_error, query


DATABASE = "cf_demand"


def page(connection, collection="events", token=None, count=100):
    cursor = {"getpage_batchCount": count}
    if token is not None:
        cursor["rt"] = token
    rows = query(
        connection,
        "SELECT documentdb_core.bson_to_json_string(document),"
        " documentdb_core.bson_to_json_string(continuation)"
        " FROM documentdb_api_internal.change_stream_aggregation"
        " (%s,%s,'{}',%s::documentdb_core.bson)",
        (DATABASE, collection, json.dumps(cursor)),
    )
    return (
        [json.loads(document) for document, _ in rows if document is not None],
        json.loads(rows[-1][1])["rt"],
    )


def insert(connection, collection, identifier):
    query(connection, "SELECT documentdb_api.insert_one(%s,%s,%s::documentdb_core.bson)",
          (DATABASE, collection, json.dumps({"_id": identifier, "counter": 0})))


def wait_for(predicate):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.1)
    raise AssertionError("Timed out waiting for demand capture maintenance")


def options(connection, collection):
    return json.loads(query(
        connection,
        "SELECT documentdb_core.bson_to_json_string(options)"
        " FROM documentdb_api_catalog.collections WHERE database_name=%s AND collection_name=%s",
        (DATABASE, collection),
    )[0][0] or "{}")


def main():
    connection = connect()
    query(connection, "CREATE EXTENSION IF NOT EXISTS documentdb_changefeed")
    assert query(connection, "SHOW documentdb_changefeed.enable")[0][0] == "on"
    assert query(connection, "SHOW documentdb_changefeed.resume_retention")[0][0] == "1d"
    for collection in ("events", "unwatched", "denied", "preserved", "rollback"):
        query(connection, "SELECT documentdb_api.create_collection(%s,%s)", (DATABASE, collection))
    insert(connection, "events", "before")
    insert(connection, "unwatched", "ordinary")
    expect_error("22023", lambda: query(connection, "ALTER SYSTEM SET documentdb_changefeed.resume_retention='25h'"))
    expect_error("0A000", lambda: page(connection, "absent"))
    assert not query(connection, "SELECT 1 FROM documentdb_api_catalog.collections"
                     " WHERE database_name=%s AND collection_name='absent'", (DATABASE,))
    query(connection, "CREATE ROLE cf_demand_denied")
    query(connection, "GRANT USAGE ON SCHEMA documentdb_core,documentdb_api_internal TO cf_demand_denied")
    query(connection, "SET ROLE cf_demand_denied")
    expect_error("42501", lambda: page(connection, "denied"))
    query(connection, "RESET ROLE")
    assert not query(connection, "SELECT 1 FROM documentdb_changefeed.feeds"
                     " WHERE database_name=%s", (DATABASE,))

    readers = Barrier(2)

    def subscribe(_):
        reader = connect()
        try:
            readers.wait(timeout=10)
            return page(reader, count=0)
        finally:
            reader.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        starts = list(pool.map(subscribe, range(2)))
    assert all(not events and token.startswith("cf2:") for events, token in starts)
    assert starts[0][1].split(":")[1] == starts[1][1].split(":")[1]
    assert query(connection, "SELECT count(*) FROM documentdb_changefeed.feeds"
                 " WHERE database_name=%s AND auto_owned", (DATABASE,))[0][0] == 1
    assert options(connection, "events")["enableUpdateDescription"] is True
    assert not options(connection, "unwatched").get("enableUpdateDescription", False)
    transaction = connect()
    transaction.autocommit = False
    insert(transaction, "unwatched", "transaction")
    expect_error("0A000", lambda: page(transaction))
    transaction.rollback()
    transaction.close()
    insert(connection, "events", "after")
    command = {"update": "events", "updates": [{"q": {"_id": "after"}, "u": {"$inc": {"counter": 2}}}]}
    result = query(connection, "SELECT documentdb_api.update(%s,%s::documentdb_core.bson,NULL)",
                   (DATABASE, json.dumps(command)))
    assert result
    events, continuation = page(connection, token=starts[0][1])
    assert [event["operationType"] for event in events] == ["insert", "update"], events
    assert events[1]["updateDescription"]["updatedFields"]["counter"] == {"$numberInt": "2"}
    assert all("wallTime" in event for event in events)
    assert page(connection, token=starts[0][1])[0] == events

    query(connection, "SELECT documentdb_api.coll_mod(%s,'preserved',"
          " '{\"collMod\":\"preserved\",\"enableUpdateDescription\":true}')", (DATABASE,))
    page(connection, "preserved", count=0)
    query(connection, "SELECT documentdb_changefeed.unregister_feed(%s,'preserved')", (DATABASE,))
    assert options(connection, "preserved")["enableUpdateDescription"] is True

    slot = query(connection, "SELECT slot FROM documentdb_changefeed.feeds"
                 " WHERE database_name=%s AND collection_name='events'", (DATABASE,))[0][0]
    old_floor = query(connection, "SELECT confirmed_flush_lsn FROM pg_replication_slots"
                      " WHERE slot_name=%s", (slot,))[0][0]
    query(connection, "ALTER SYSTEM SET documentdb_changefeed.resume_retention='2s'")
    query(connection, "SELECT pg_reload_conf()")
    try:
        wait_for(lambda: query(connection, "SHOW documentdb_changefeed.resume_retention")[0][0] == "2s")
        transaction = connect()
        transaction.autocommit = False
        page(transaction, "rollback", count=0)
        transaction.rollback()
        transaction.close()
        wait_for(lambda: not query(connection, "SELECT 1 FROM documentdb_changefeed.pending_slots"))
        assert not options(connection, "rollback").get("enableUpdateDescription", False)
        expect_error("55000", lambda: page(connection, token=continuation.rsplit(":", 1)[0] + ":1"))
        # Keep one reader alive while its old resume history ages out.
        recent = continuation
        deadline = time.monotonic() + 4
        while time.monotonic() < deadline:
            _, recent = page(connection, token=recent)
            time.sleep(0.3)
        expect_error("55000", lambda: page(connection, token=starts[0][1]))
        wait_for(lambda: query(connection, "SELECT confirmed_flush_lsn > %s::pg_lsn"
                               " FROM pg_replication_slots WHERE slot_name=%s", (old_floor, slot))[0][0])
        insert(connection, "events", "retained")
        assert page(connection, token=recent)[0][0]["documentKey"]["_id"] == "retained"
        wait_for(lambda: not query(connection, "SELECT 1 FROM documentdb_changefeed.feeds"
                                    " WHERE database_name=%s", (DATABASE,)))
        assert not query(connection, "SELECT 1 FROM pg_replication_slots WHERE slot_name=%s", (slot,))
        assert not options(connection, "events").get("enableUpdateDescription", False)
        expect_error("55000", lambda: page(connection, token=recent))
        fresh = page(connection, count=0)[1]
        assert fresh.split(":")[1] != recent.split(":")[1]
        query(connection, "SELECT documentdb_changefeed.unregister_feed(%s,'events')", (DATABASE,))
    finally:
        query(connection, "ALTER SYSTEM RESET documentdb_changefeed.resume_retention")
        query(connection, "SELECT pg_reload_conf()")
        query(connection, "REVOKE USAGE ON SCHEMA documentdb_core,documentdb_api_internal FROM cf_demand_denied")
        query(connection, "DROP ROLE cf_demand_denied")
        connection.close()
    print("PASS demand capture: concurrent first watch, authorization, untouched namespaces,"
          " updates, replay, retention advance, expiry, cleanup and new generation")


if __name__ == "__main__":
    main()
