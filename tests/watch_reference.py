"""Launch the packaged gateway and prove the existing watch/cursor seam."""

import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import time

from fixtures import connect, query


def main():
    connection = connect()
    password = secrets.token_urlsafe(24)
    query(connection, "SELECT documentdb_api.create_collection('cf_watch','events')")
    query(connection, "SELECT documentdb_api.delete('cf_watch',"
          " '{\"delete\":\"events\",\"deletes\":[{\"q\":{},\"limit\":0}]}')")
    query(connection, "SELECT documentdb_api.create_collection('cf_watch_updates','events')")
    query(connection, "SELECT documentdb_api.create_user(%s::documentdb_core.bson)",
          (json.dumps({"createUser": "cf_client", "pwd": password, "roles": [
              {"role": "readWriteAnyDatabase", "db": "admin"},
              {"role": "clusterAdmin", "db": "admin"}]}),))
    home = Path("/home/documentdb/gateway/pg_documentdb_gw")
    config = json.loads((home / "SetupConfiguration.json").read_text())
    config.update({"PostgresPort": 5432, "GatewayListenPort": 10260,
                   "PostgresSystemUser": "postgres", "PostgresDataUser": "postgres",
                   "UseLocalHost": True})
    path = Path("/tmp/cf-gateway-config.json")
    path.write_text(json.dumps(config))
    path.chmod(0o600)
    with Path("/tmp/cf-gateway.log").open("w") as log:
        gateway = subprocess.Popen(
            [str(home / "target/release-with-symbols/documentdb_gateway"), str(path)],
            stdout=log, stderr=subprocess.STDOUT,
        )
        try:
            for _ in range(60):
                if gateway.poll() is not None:
                    raise RuntimeError("Gateway exited: " + Path("/tmp/cf-gateway.log").read_text()[-5000:])
                try:
                    with socket.create_connection(("localhost", 10260), timeout=1):
                        break
                except OSError:
                    time.sleep(0.5)
            else:
                raise RuntimeError("Gateway did not become ready")
            subprocess.run(
                ["mongosh", "--quiet", "--host", "localhost", "--port", "10260",
                 "--tls", "--tlsAllowInvalidCertificates", "--file", "tests/watch.js"],
                env={**os.environ, "CF_TEST_PASSWORD": password}, check=True, timeout=120,
            )
            functional_tests = os.environ.get("CHANGEFEED_FUNCTIONAL_TESTS")
            if functional_tests:
                for database in ("cf_watch", "cf_watch_updates"):
                    query(connection, "SELECT documentdb_changefeed.unregister_feed(%s,'events')", (database,))
                uri = (f"mongodb://cf_client:{password}@localhost:10260/"
                       "?tls=true&tlsAllowInvalidCertificates=true&authSource=admin&directConnection=true")
                from pymongo import MongoClient

                client = MongoClient(uri, serverSelectionTimeoutMS=5000)
                try:
                    for command in ("hello", "whatsmyuri"):
                        try:
                            reply = client.admin.command(command)
                            if command == "whatsmyuri" and not isinstance(reply.get("you"), str):
                                raise RuntimeError("response is missing the required string field 'you'")
                        except Exception as error:
                            raise RuntimeError(
                                f"Public suite capability probe {command} failed: "
                                + str(error).replace(password, "<redacted>")
                            ) from None
                finally:
                    client.close()
                tests = [
                    f"documentdb_tests/compatibility/tests/changeStreams/{operation}/"
                    f"test_smoke_changeStream_{operation}.py"
                    for operation in ("insert", "update", "replace", "delete")
                ]
                result = subprocess.run(
                    ["pytest", *tests, "-q", "--tb=short",
                     "-o", "cache_dir=/tmp/cf-functional-pytest-cache",
                     "--engine-name", "documentdb", "--connection-string", uri],
                    cwd=functional_tests, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, timeout=180,
                )
                print(result.stdout.replace(password, "<redacted>"), flush=True)
                if result.returncode:
                    raise RuntimeError(f"Public change-stream smoke tests exited {result.returncode}")
        finally:
            gateway.terminate()
            try:
                gateway.wait(timeout=10)
            except subprocess.TimeoutExpired:
                gateway.kill()
                gateway.wait()
            path.unlink()
            query(connection, "SELECT documentdb_changefeed.unregister_feed('cf_watch','events')")
            query(connection, "SELECT documentdb_changefeed.unregister_feed('cf_watch_updates','events')")
            connection.close()


if __name__ == "__main__":
    main()
