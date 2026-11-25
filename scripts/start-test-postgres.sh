#!/usr/bin/env bash
set -euo pipefail

if [[ ! -f /tmp/changefeed-pg/PG_VERSION ]]; then
    initdb -D /tmp/changefeed-pg -U postgres -A trust >/dev/null
fi
preload=pg_documentdb_core,pg_documentdb,pg_cron
enabled=off
options=()
if [[ ${CHANGEFEED_LOGICAL_TEST:-0} == 1 ]]; then
    preload+=,pg_documentdb_changefeed
    enabled=on
    options+=(-c output_plugin_libraries=pgoutput,pg_documentdb_changefeed)
fi
exec postgres -D /tmp/changefeed-pg -k /tmp -p 5432 \
    -c listen_addresses=localhost \
    -c wal_level=logical -c max_replication_slots=4 \
    -c max_slot_wal_keep_size=1GB \
    -c shared_preload_libraries="$preload" \
    -c documentdb_changefeed.enable="$enabled" \
    -c cron.database_name=postgres "${options[@]}"
