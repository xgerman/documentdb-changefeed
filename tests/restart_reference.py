"""Verify token replay across an ordinary restart of the same primary."""

import json
from pathlib import Path
import sys

from fixtures import connect, query


TOKEN = Path("/tmp/cf-restart-token.json")


def main():
    connection = connect()
    if sys.argv[1] == "prepare":
        tokens = {}
        for database in ("cf_restart", "cf_restart_owned"):
            query(connection, "SELECT documentdb_api.create_collection(%s,'events')", (database,))
            if database == "cf_restart":
                query(connection, "SELECT documentdb_api.coll_mod(%s,'events',"
                      " '{\"collMod\":\"events\",\"enableUpdateDescription\":true}')", (database,))
                query(connection, "SELECT pg_create_logical_replication_slot('cf_restart','pg_documentdb_changefeed')")
                query(connection, "SELECT documentdb_changefeed.register_feed(%s,'events','cf_restart',true)", (database,))
            tokens[database] = json.loads(query(
                connection, "SELECT documentdb_core.bson_to_json_string(continuation)"
                " FROM documentdb_api_internal.change_stream_aggregation(%s,'events','{}','{}')",
                (database,))[0][0])
            query(connection, "SELECT documentdb_api.insert_one(%s,'events','{\"_id\":\"retained\"}')", (database,))
            for update in ({"$set": {"updated": True}}, {"_id": "retained", "payload": "restart"}):
                result = query(connection, "SELECT p_success FROM documentdb_api.update"
                               " (%s,%s::documentdb_core.bson)",
                               (database, json.dumps({"update": "events", "updates": [
                                   {"q": {"_id": "retained"}, "u": update}]})))
                assert result[0][0], result
        TOKEN.write_text(json.dumps(tokens))
    else:
        for database, token in json.loads(TOKEN.read_text()).items():
            rows = query(connection, "SELECT documentdb_core.bson_to_json_string(document)"
                         " FROM documentdb_api_internal.change_stream_aggregation"
                         " (%s,'events','{}',%s::documentdb_core.bson)", (database, json.dumps(token)))
            events = [json.loads(row[0]) for row in rows if row[0] is not None]
            assert [event["operationType"] for event in events] == ["insert", "update", "replace"], events
            assert all(event["documentKey"]["_id"] == "retained" for event in events)
            assert events[1]["updateDescription"]["updatedFields"] == {"updated": True}
            assert events[2]["fullDocument"]["payload"] == "restart"
            query(connection, "SELECT documentdb_changefeed.unregister_feed(%s,'events')", (database,))
        query(connection, "SELECT pg_drop_replication_slot('cf_restart')")
        TOKEN.unlink()
        print("PASS manual and demand-owned retained token replay after primary restart")
    connection.close()


if __name__ == "__main__":
    main()
