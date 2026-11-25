# Run the logical reference in DocumentDB Local

Use a disposable environment. This is an insert/delete and opt-in update `watch()` proof, not a
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
restarting the same primary.

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
ALTER SYSTEM SET output_plugin_libraries = pgoutput, pg_documentdb_changefeed;
ALTER SYSTEM SET documentdb_changefeed.enable = on;
```

Preserve any additional existing preload libraries. The provider must come
after DocumentDB. Restart PostgreSQL and the gateway, then verify the effective
settings. On PostgreSQL versions with an output-plugin allowlist, explicitly
trusting this library is required; keep replication privileges restricted to
administrators.

Installation fails rather than overwrite an existing change-stream UDF.
Preloading fails rather than chain or replace an existing availability hook.
Installing the extension does not enable the default-off feature.

## Register one existing collection

Create the collection before the slot. Each SQL statement below must run in a
separate autocommit transaction; slot creation cannot follow writes in the same
transaction.

```sql
SELECT documentdb_api.create_collection('cf_demo', 'events');
SELECT pg_create_logical_replication_slot('cf_demo', 'pg_documentdb_changefeed');
SELECT documentdb_changefeed.register_feed('cf_demo', 'events', 'cf_demo');
```

Registration attaches an existing slot, validates its plugin/database and the
unsharded collection, and records a generation and relation identity. It does
not create slots or alter replica identity. The serving function checks the
invoking role's collection read permission on every call.

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

## Opt into update capture

For a new feed that includes updates, replace the three-argument registration
above with:

```sql
SELECT documentdb_api.coll_mod(
    'cf_demo', 'events', '{"collMod":"events","enableUpdateDescription":true}');
SELECT documentdb_changefeed.register_feed('cf_demo', 'events', 'cf_demo', true);
```

The library must be preloaded after DocumentDB in every writing backend.
Registration installs a generation-specific AFTER UPDATE trigger. Operator
events contain applied `updateDescription`; replacements and pipelines contain
the exact replacement `fullDocument`. No-op operator updates emit nothing.
Disabling the serving GUC does not stop capture for registered feeds.
Unregister an existing feed before changing its capture policy; that ends its
generation and invalidates its old tokens.

Use a fresh installation for this unreleased reference schema. In-place
upgrades from earlier experimental installations are not supported.

## Close the feed

Unregistering detaches metadata but deliberately does not drop an
administrator's slot:

```sql
SELECT documentdb_changefeed.unregister_feed('cf_demo', 'events');
SELECT pg_drop_replication_slot('cf_demo');
```

Do not leave unattended slots. Peeking does not advance retained history.
Reads reject a retained scan above 256 MiB; decoding rejects output above
64 MiB and serving respects event/page bounds. `max_slot_wal_keep_size` is a
checkpoint-enforced limit, not an instantaneous disk cap.

Unregistered updates, positional mutations, truncation, pre-images, timestamp starts, distributed
collections, archive access, and durable failover are not supported by this
milestone. Tokens are opaque and scope-checked, not authenticated production
credentials. Lost, externally advanced, or replaced slot history fails rather
than reset to now. See [the design](logical-reference.md) for the remaining
production gates and [the update design](update-events.md) for capture limits.
