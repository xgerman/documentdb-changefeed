"""Verify token replay across an ordinary restart of the same primary."""

import json
from pathlib import Path
import sys

from fixtures import connect, query


TOKEN = Path("/tmp/cf-restart-token.json")


def main():
    connection = connect()
    if sys.argv[1] == "prepare":
        query(connection, "SELECT documentdb_api.create_collection('cf_restart','events')")
        query(connection, "SELECT documentdb_api.coll_mod('cf_restart','events',"
              " '{\"collMod\":\"events\",\"enableUpdateDescription\":true}')")
        query(connection, "SELECT pg_create_logical_replication_slot('cf_restart','pg_documentdb_changefeed')")
        query(connection, "SELECT documentdb_changefeed.register_feed('cf_restart','events','cf_restart',true)")
        token = query(connection, "SELECT documentdb_core.bson_to_json_string(continuation)"
                      " FROM documentdb_api_internal.change_stream_aggregation('cf_restart','events','{}','{}')")[0][0]
        TOKEN.write_text(token)
        query(connection, "SELECT documentdb_api.insert_one('cf_restart','events','{\"_id\":\"retained\"}')")
        for update in ({"$set": {"updated": True}}, {"_id": "retained", "payload": "restart"}):
            result = query(connection, "SELECT p_success FROM documentdb_api.update"
                           " ('cf_restart',%s::documentdb_core.bson)",
                           (json.dumps({"update": "events", "updates": [
                               {"q": {"_id": "retained"}, "u": update}]}),))
            assert result[0][0], result
    else:
        rows = query(connection, "SELECT documentdb_core.bson_to_json_string(document)"
                     " FROM documentdb_api_internal.change_stream_aggregation"
                     " ('cf_restart','events','{}',%s::documentdb_core.bson)", (TOKEN.read_text(),))
        events = [json.loads(row[0]) for row in rows if row[0] is not None]
        assert [event["operationType"] for event in events] == ["insert", "update", "replace"], events
        assert all(event["documentKey"]["_id"] == "retained" for event in events)
        assert events[1]["updateDescription"]["updatedFields"] == {"updated": True}
        assert events[2]["fullDocument"]["payload"] == "restart"
        query(connection, "SELECT documentdb_changefeed.unregister_feed('cf_restart','events')")
        query(connection, "SELECT pg_drop_replication_slot('cf_restart')")
        TOKEN.unlink()
        print("PASS retained token replay after primary restart")
    connection.close()


if __name__ == "__main__":
    main()
