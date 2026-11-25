#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
runtime="${CONTAINER_RUNTIME:-docker}"
major="${PG_MAJOR:-17}"
logical="${CHANGEFEED_LOGICAL_TEST:-0}"
if [[ "$logical" == 1 && ( "$major" != 17 || -z "${DOCUMENTDB_IMAGE:-}" ) ]]; then
    echo "Logical watch tests require PG_MAJOR=17 and DOCUMENTDB_IMAGE" >&2
    exit 1
fi
case "$major" in
    16|17|18) ;;
    *) echo "Unsupported PostgreSQL major: $major" >&2; exit 1 ;;
esac

image="localhost/documentdb-changefeed-test:pg${major}"
name="documentdb-changefeed-pg${major}-$$"
user=postgres
test=tests/fixtures.py
build_args=(--build-arg "PG_MAJOR=$major")
mounts=()
if [ -n "${DOCUMENTDB_SOURCE:-}" ]; then
    source_dir="$(cd "$DOCUMENTDB_SOURCE" && pwd)"
    mounts+=(-v "$source_dir:/tmp/documentdb-source:ro")
fi
if [ -n "${DOCUMENTDB_IMAGE:-}" ]; then
    image="localhost/documentdb-changefeed-collections:pg${major}"
    user=documentdb
    test=tests/documentdb_collections.py
    build_args+=(-f tests/Dockerfile.documentdb --build-arg "DOCUMENTDB_IMAGE=$DOCUMENTDB_IMAGE")
fi
log="$(mktemp)"
cleanup() {
    status=$?
    if [ "$status" -ne 0 ]; then
        "$runtime" logs "$name" 2>/dev/null || true
    fi
    "$runtime" rm -f "$name" >/dev/null 2>&1 || true
    rm -f "$log"
    exit "$status"
}
trap cleanup EXIT

if ! "$runtime" build "${build_args[@]}" -t "$image" . >"$log" 2>&1; then
    cat "$log"
    exit 1
fi
if [ -n "${DOCUMENTDB_IMAGE:-}" ]; then
    "$runtime" run -d --name "$name" "${mounts[@]}" -e PGHOST=/tmp \
        -e "PG_MAJOR=$major" -e "CHANGEFEED_LOGICAL_TEST=$logical" "$image" >/dev/null
else
    "$runtime" run -d --name "$name" \
        "${mounts[@]}" \
        -e POSTGRES_HOST_AUTH_METHOD=trust \
        "$image" postgres -c wal_level=logical -c max_replication_slots=4 \
        -c max_slot_wal_keep_size=1GB >/dev/null
fi
ready=false
for ((attempt=0; attempt<60; attempt++)); do
    if "$runtime" exec "$name" pg_isready -U postgres >/dev/null 2>&1; then
        # Image initialization uses a temporary server; wait for its final restart.
        if [ -n "${DOCUMENTDB_IMAGE:-}" ] ||
           "$runtime" logs "$name" 2>&1 | grep -q "PostgreSQL init process complete"; then
            ready=true
            break
        fi
    fi
    sleep 1
done
if [ "$ready" != true ]; then
    echo "PostgreSQL $major did not become ready" >&2
    exit 1
fi
"$runtime" exec --user "$user" "$name" /usr/bin/python3 "$test"
if [ "$logical" == 1 ]; then
    "$runtime" exec --user "$user" "$name" /usr/bin/python3 tests/logical_reference.py
    "$runtime" exec --user "$user" "$name" /usr/bin/python3 tests/watch_reference.py
    "$runtime" exec --user "$user" "$name" /usr/bin/python3 tests/restart_reference.py prepare
    "$runtime" restart "$name" >/dev/null
    for ((attempt=0; attempt<60; attempt++)); do
        if "$runtime" exec "$name" pg_isready -U postgres >/dev/null 2>&1; then
            break
        fi
        sleep 1
    done
    "$runtime" exec --user "$user" "$name" /usr/bin/python3 tests/restart_reference.py check
fi
if [ -n "${DOCUMENTDB_SOURCE:-}" ]; then
    "$runtime" exec "$name" sh -c \
        'cc -fsyntax-only -I"$(pg_config --includedir-server)" -I/tmp/documentdb-source/pg_documentdb_core/include -I/tmp/documentdb-source/pg_documentdb/include $(pkg-config --cflags libbson-1.0) tests/check_headers.c'
    echo "PASS public DocumentDB headers on PostgreSQL $major"
fi
