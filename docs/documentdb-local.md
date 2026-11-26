# Run the logical reference in DocumentDB Local

Use a disposable environment. This is a demand-driven collection `watch()` proof, not a
production changefeed. It uses the packaged DocumentDB backend and gateway,
without the bundled core patch or FULL replica identity.

## Reproduce the complete proof

From this repository's root:

```sh
CONTAINER_RUNTIME=podman PG_MAJOR=17 \
  DOCUMENTDB_IMAGE=ghcr.io/documentdb/documentdb/documentdb-local:pg17-0.117.0 \
  CHANGEFEED_LOGICAL_TEST=1 bash scripts/test-container.sh
```

Use `CONTAINER_RUNTIME=docker` for Docker. All compilers, PostgreSQL tools,
Python dependencies, and the client run inside containers. The runner exposes
no host ports and removes only its own container.

It verifies the original native collection fixtures, the logical serving UDF,
authenticated client `watch()`/`getMore`, independent readers, filtering,
exclusive resume, operator updates, replacements, no-ops, and replay after
restarting the same primary. Client fixtures do not register feeds or create
slots. Separate regressions cover retention, cleanup, and schema upgrade.

The test image installs the BSON runtime library and updates PostgreSQL within
major 17 to match the current build headers. It does not rebuild or replace the
DocumentDB backend or gateway. A different PostgreSQL major or CPU architecture
requires its own build.

## Build a normal Local image

The `local` target preserves the upstream gateway entrypoint. The default final
target starts a private SQL test server instead.

```sh
podman build -f tests/Dockerfile.documentdb --target local \
  --build-arg PG_MAJOR=17 \
  --build-arg DOCUMENTDB_IMAGE=ghcr.io/documentdb/documentdb/documentdb-local:pg17-0.117.0 \
  -t localhost/documentdb-local-changefeed:pg17 .
```

Start this image using the upstream Local image's documented flags, a fresh
data directory, and disposable credentials. Do not replace libraries in a
running server or reuse a production data directory.

## Configure a separately started server

The automated runner supplies these settings itself. For a separate fresh
server, use a PostgreSQL superuser in the gateway's `postgres` database:

```sql
CREATE EXTENSION documentdb_changefeed CASCADE;
SHOW shared_preload_libraries;
ALTER SYSTEM SET shared_preload_libraries =
    pg_documentdb_core, pg_documentdb, pg_cron, pg_documentdb_changefeed;
ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_replication_slots = 4;
ALTER SYSTEM SET max_slot_wal_keep_size = '1GB';
ALTER SYSTEM SET max_worker_processes = 8;
ALTER SYSTEM SET output_plugin_libraries = pgoutput, pg_documentdb_changefeed;
ALTER SYSTEM SET documentdb_changefeed.enable = on;
ALTER SYSTEM SET documentdb_changefeed.resume_retention = '24h';
```

Preserve any additional existing preload libraries. The provider must come
after DocumentDB. Restart PostgreSQL and the gateway, then verify the effective
settings. On PostgreSQL versions with an output-plugin allowlist, explicitly
trusting this library is required; keep replication privileges restricted to
administrators.

Installation fails rather than overwrite an existing change-stream UDF.
Preloading fails rather than chain or replace an existing availability hook.
The serving capability defaults to enabled after installation. Capture remains
inactive until a collection is watched. The background launcher and one
maintenance worker per database with retained demand require worker capacity.

## Watch one existing collection

Create the collection normally, through the client or the collection API.
An absent-namespace watch is an explicit feature gap; it does not create a
collection as a side effect.

```sql
SELECT documentdb_api.create_collection('cf_demo', 'events');
```

The first executed watch automatically provisions an owned slot, update capture,
and a generation. It does not alter replica identity. The serving function
checks the invoking role's collection read permission before activation and on
every read. No additional SQL registration is required.

In an authenticated client connected to that gateway:

```javascript
const events = db.getSiblingDB("cf_demo").events;
const stream = events.watch([], { batchSize: 1, maxAwaitTimeMS: 50 });
stream.tryNext(); // Establish the initial start-from-now checkpoint.
events.insertOne({ _id: "example", value: 42 });
const inserted = stream.tryNext();
events.deleteOne({ _id: "example" });
const deleted = stream.tryNext();
stream.close();
```

Subscription is established when the cursor is first executed, not when a
client object is constructed. Save an event's `_id` or the cursor's
post-batch resume token and pass it as `resumeAfter` to replay exclusively after
that position, while the same registered history survives.

## Retention and cleanup

History and idle capture default to at most 24 hours. A shorter policy can be
applied without a server restart:

```sql
ALTER SYSTEM SET documentdb_changefeed.resume_retention = '6h';
SELECT pg_reload_conf();
```

The library must be preloaded after DocumentDB in every writing backend.
Activation installs a generation-specific AFTER UPDATE trigger. Operator
events contain applied `updateDescription`; replacements and pipelines contain
the exact replacement `fullDocument`. No-op operator updates emit nothing.
Disabling the serving GUC does not stop capture for registered feeds.
Successful polls renew demand. Closing a cursor does not immediately stop
capture, because disconnected clients need retained resume history.
Cleanup runs periodically, up to once per minute at the default retention.

Upgrade an existing 0.1.0 schema to 0.2.0:

Install the new library and SQL files, restart PostgreSQL and the gateway to
load the new preloaded binary, then upgrade the schema before serving watches.

```sql
ALTER EXTENSION documentdb_changefeed UPDATE;
```

An administrator can end demand immediately:

```sql
SELECT documentdb_changefeed.unregister_feed('cf_demo', 'events');
```

This removes owned capture and its slot. Unregister owned feeds before dropping
the extension itself; SQL removal does not preserve their cleanup metadata.

## Optional manual diagnostics

The original manual API remains available. Statements must use separate
autocommit transactions when creating logical slots:

```sql
SELECT pg_create_logical_replication_slot('cf_demo', 'pg_documentdb_changefeed');
SELECT documentdb_changefeed.register_feed('cf_demo', 'events', 'cf_demo');
SELECT documentdb_changefeed.unregister_feed('cf_demo', 'events');
SELECT pg_drop_replication_slot('cf_demo');
```

Manual slots are never advanced or dropped automatically. Do not leave them
unattended.
Reads reject a retained scan above 256 MiB; decoding rejects output above
64 MiB and serving respects event/page bounds. `max_slot_wal_keep_size` is a
checkpoint-enforced limit, not an instantaneous disk cap.

Unregistered updates, positional mutations, truncation, pre-images, timestamp starts, distributed
collections, archive access, and durable failover are not supported by this
milestone. Tokens are opaque and scope-checked, not authenticated production
credentials. Lost, externally advanced, or replaced slot history fails rather
than reset to now. See [the design](logical-reference.md) for the remaining
production gates and [the update design](update-events.md) for capture limits.
