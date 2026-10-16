#!/usr/bin/env bash
set -euo pipefail

initdb -D /tmp/changefeed-pg -U postgres -A trust >/dev/null
exec postgres -D /tmp/changefeed-pg -k /tmp -p 5432 \
    -c listen_addresses=localhost \
    -c wal_level=logical -c max_replication_slots=4 \
    -c max_slot_wal_keep_size=1GB \
    -c shared_preload_libraries=pg_documentdb_core,pg_documentdb,pg_cron \
    -c cron.database_name=postgres
