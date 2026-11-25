"""Verify applied update metadata and replacement images through retained WAL."""

import base64
import json
import random
from concurrent.futures import ThreadPoolExecutor

from fixtures import connect, expect_error, query


DATABASE = "cf_updates"


def command(connection, update, identifier="one", multi=False, error=None, selector=None):
    rows = query(connection, "SELECT documentdb_core.bson_to_json_string(p_result),p_success"
                 " FROM documentdb_api.update(%s,%s::documentdb_core.bson)",
                 (DATABASE, json.dumps({"update": "events", "updates": [
                     {"q": selector if selector is not None else ({} if multi else {"_id": identifier}),
                      "u": update, "multi": multi}]})))
    result = numeric(json.loads(rows[0][0]))
    if error is None:
        assert rows[0][1] and "writeErrors" not in result, result
    else:
        assert not rows[0][1] and error in result["writeErrors"][0]["errmsg"], result
    return result


def read(connection, token=None, count=100):
    cursor = {"getpage_batchCount": count, "getpage_batchSize": 16 * 1024 * 1024}
    if token is not None:
        cursor["rt"] = token
    rows = query(connection, "SELECT documentdb_core.bson_to_json_string(document),"
                 " documentdb_core.bson_to_json_string(continuation)"
                 " FROM documentdb_api_internal.change_stream_aggregation"
                 " (%s,'events','{}',%s::documentdb_core.bson)",
                 (DATABASE, json.dumps(cursor)))
    return [json.loads(row[0]) for row in rows if row[0]], json.loads(rows[-1][1])["rt"]


def numeric(value):
    if isinstance(value, dict) and set(value) in ({"$numberInt"}, {"$numberLong"}):
        return int(next(iter(value.values())))
    if isinstance(value, dict):
        return {key: numeric(child) for key, child in value.items()}
    if isinstance(value, list):
        return [numeric(child) for child in value]
    return value


def main():
    connection = connect()
    query(connection, "SELECT documentdb_api.create_collection(%s,'events')", (DATABASE,))
    query(connection, "SELECT pg_create_logical_replication_slot('cf_updates','pg_documentdb_changefeed')")
    expect_error("55000", lambda: query(connection,
                 "SELECT documentdb_changefeed.register_feed(%s,'events','cf_updates',true)", (DATABASE,)))
    query(connection, "SELECT documentdb_api.coll_mod(%s,'events','{\"collMod\":\"events\","
          "\"enableUpdateDescription\":true}')", (DATABASE,))
    query(connection, "SELECT documentdb_changefeed.register_feed(%s,'events','cf_updates',true)", (DATABASE,))
    relation = query(connection, "SELECT relation_id::regclass::text FROM documentdb_changefeed.feeds"
                     " WHERE database_name=%s", (DATABASE,))[0][0]
    assert query(connection, "SELECT relreplident FROM pg_class WHERE oid=%s::regclass", (relation,))[0][0] == "d"
    query(connection, "SELECT documentdb_api.insert_one(%s,'events',"
          " '{\"_id\":\"one\",\"counter\":0,\"remove\":1,\"nested\":{\"value\":0},\"items\":[1,2]}')",
          (DATABASE,))
    _, start = read(connection)
    command(connection, {"$set": {"nested.value": 3}, "$inc": {"counter": 2}, "$unset": {"remove": ""}})
    command(connection, {"$push": {"items": 3}})
    command(connection, {"$set": {"items.0": 4}})
    operator, continuation = read(connection, start)
    assert len(operator) == 3
    assert all(event["operationType"] == "update" and "fullDocument" not in event for event in operator)
    first = numeric(operator[0]["updateDescription"])
    assert first["updatedFields"] == {"counter": 2, "nested.value": 3}, first
    assert first["removedFields"] == ["remove"] and first["truncatedArrays"] == []
    assert numeric(operator[1]["updateDescription"]["updatedFields"]) == {"items": [1, 2, 3]}
    assert numeric(operator[2]["updateDescription"]["updatedFields"]) == {"items.0": 4}

    command(connection, {"$set": {"counter": 2}})
    command(connection, {"$set": {"counter": 2}}, multi=True)
    noop, continuation = read(connection, continuation)
    assert noop == [], noop

    transaction = connect()
    transaction.autocommit = False
    command(transaction, {"$inc": {"counter": 1}})
    query(transaction, "SAVEPOINT discard")
    command(transaction, {"$set": {"counter": 99}})
    query(transaction, "ROLLBACK TO SAVEPOINT discard")
    command(transaction, {"$inc": {"counter": 1}})
    transaction.commit()
    multiple, continuation = read(connection, continuation)
    assert [numeric(e["updateDescription"]["updatedFields"]) for e in multiple] == [
        {"counter": 3}, {"counter": 4}
    ], multiple
    query(transaction, "SAVEPOINT released")
    command(transaction, {"$set": {"counter": 100}})
    query(transaction, "RELEASE SAVEPOINT released")
    transaction.rollback()
    assert read(connection, continuation)[0] == []
    transaction.close()

    replacement = {"_id": "one", "payload": random.Random(113).randbytes(100000).hex()}
    assert command(connection, replacement)["nModified"] == 1
    assert command(connection, replacement)["nModified"] == 1
    command(connection, [{"$set": {"pipeline": True}}])
    replaced, continuation = read(connection, continuation)
    assert len(replaced) == 3 and all(e["operationType"] == "replace" for e in replaced), [
        (event["operationType"], list(event.get("fullDocument", {}))) for event in replaced
    ]
    assert all(e["fullDocument"]["payload"] == replacement["payload"] for e in replaced)
    assert replaced[-1]["fullDocument"]["pipeline"] is True
    assert all("updateDescription" not in event for event in replaced)

    query(connection, "SELECT documentdb_api.insert_one(%s,'events','{\"_id\":1,\"counter\":0}')", (DATABASE,))
    query(connection, "SELECT documentdb_api.insert_one(%s,'events','{\"_id\":\"1\",\"counter\":0}')", (DATABASE,))
    _, many_start = read(connection)
    command(connection, {"$set": {"shared": True}}, multi=True)
    many, continuation = read(connection, many_start)
    assert len(many) == 3 and all(e["updateDescription"]["updatedFields"] == {"shared": True} for e in many)
    assert len({json.dumps(event["documentKey"]["_id"]) for event in many}) == 3
    paged = []
    token = start
    for _ in range(30):
        events, token = read(connection, token, count=1)
        paged.extend(events)
        if not events:
            break
    replay, _ = read(connection, start)
    assert paged == replay
    assert base64.b64decode(replay[0]["_id"]["_data"]).decode().startswith("cf1:")
    def replay_reader(_):
        reader = connect()
        try:
            return read(reader, start)[0]
        finally:
            reader.close()
    with ThreadPoolExecutor(max_workers=2) as pool:
        assert list(pool.map(replay_reader, range(2))) == [replay, replay]

    query(connection, "SET documentdb_changefeed.enable=off")
    command(connection, {"$set": {"disabled_gate": True}})
    observer = connect()
    try:
        captured, continuation = read(observer, continuation)
        assert captured[0]["updateDescription"]["updatedFields"] == {"disabled_gate": True}
    finally:
        observer.close()
        query(connection, "RESET documentdb_changefeed.enable")

    command(connection, {"$set": {"array": [1, 2]}})
    command(connection, {"$set": {"array.$[]": 9}},
            error="reference update capture does not support positional updates")
    expect_error("0A000", lambda: query(connection, f"UPDATE {relation} SET document="
                 " '{\"_id\":\"one\",\"raw\":true}'::documentdb_core.bson"))

    connection.autocommit = False
    query(connection, "SELECT documentdb_api.insert_one(%s,'events',"
          " json_build_object('_id',identifier)::text::documentdb_core.bson)"
          " FROM generate_series(10000,14095) identifier", (DATABASE,))
    command(connection, {"$set": {"cap_check": True}}, multi=True,
            error="oversized reference update ledger")
    bounded = command(connection, {"$set": {"cap_recovered": True}}, multi=True,
                      selector={"_id": {"$gte": 10000, "$lt": 14096}})
    assert bounded["nModified"] == 4096, bounded
    connection.commit()
    connection.autocommit = True
    assert read(connection, continuation, count=1)[0]
    query(connection, "SELECT documentdb_changefeed.unregister_feed(%s,'events')", (DATABASE,))
    assert query(connection, "SELECT count(*) FROM pg_trigger WHERE tgrelid=%s::regclass"
                 " AND tgname LIKE 'cf_update_%%'", (relation,))[0][0] == 0
    command(connection, {"$set": {"after_unregister": True}})
    query(connection, "SELECT pg_drop_replication_slot('cf_updates')")
    connection.close()
    print("PASS update reference: applied fields/removals, nested/numeric paths, arrays, "
          "replacements/pipelines, exact large images, physical no-ops, update-many, "
          "commit/savepoint rollback, typed keys, replay/pages, capture-policy checks and cleanup")


if __name__ == "__main__":
    main()
