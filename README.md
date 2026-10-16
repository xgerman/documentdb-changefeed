# DocumentDB Changefeed

Experimental PostgreSQL extension foundation for a retained-WAL DocumentDB
changefeed. PostgreSQL 16, 17, and 18 are supported build targets, with a separate
binary for each major. This is **not a usable production changefeed** yet.

The PGXS package declares dependencies on `documentdb_core` and `documentdb`.
Its installation SQL currently exposes only a restricted `version()` function.
There is no capture policy, managed retention, continuation token, public feed,
or MongoDB change-stream provider yet.

## Install into DocumentDB Local

See [the container installation guide](docs/documentdb-local.md) for installing
the module into a normal DocumentDB Local image, building the companion core
patch, enabling logical WAL, and checking SQL installation and MongoDB access.
The `local` Dockerfile target preserves the upstream gateway entrypoint; the
default target is only for SQL fixtures.

## Decoder feasibility fixtures

The private, superuser-only fixture decoder scans retained local WAL once per
invocation. It reconstructs committed heap events, including multi-insert row
ordinals, aborts/subtransactions, and historical compressed/uncompressed TOAST
images. It never queries the live heap/TOAST table for historical document bytes.
An in-scan prior image is reused only when its physical TOAST identity matches.
Missing historical chunks fail explicitly.

The decoder accepts actual DocumentDB collection relations with `object_id`
and `document` columns of the extension-owned `documentdb_core.bson` type.
Row-image state uses the complete BSON `object_id`, never the non-unique shard
key; hash collisions are resolved by exact byte comparison. It also accepts a
plain fixture relation with a unique `bigint` first column and a non-null
`bytea` `document` containing BSON bytes. Both require `REPLICA IDENTITY FULL`.

The real-collection suite installs the extension against DocumentDB 0.117-0,
creates a collection through `documentdb_api.create_collection`, and writes
through `insert_one`, `update`, and `delete`. It snapshots stored BSON bytes as
an oracle during writes, then verifies historical decoding after updates and
deletion. Coverage includes overlapping commits, aborts/savepoints, compressed
and incompressible external BSON, repeated shard keys, numeric versus string
IDs, replay, permissions, and installation/removal.

The real-collection integration image currently runs PostgreSQL 17. Separate
PG16/17/18 fixtures cover native WAL formats, multi-inserts, unchanged external
attributes, explicit missing-history errors, and memory/range bounds. Live
DocumentDB collection integration on PG16 and PG18 remains unverified.

Both suites explicitly install a private diagnostic C function; it is not
included in the extension's installation SQL. No dependency extensions are
faked. This is not MongoDB driver or gateway `watch()` coverage.

Run dependencies only in containers:

```sh
PG_MAJOR=16 bash scripts/test-container.sh
PG_MAJOR=17 bash scripts/test-container.sh
PG_MAJOR=18 bash scripts/test-container.sh
# Use Podman locally:
CONTAINER_RUNTIME=podman PG_MAJOR=18 bash scripts/test-container.sh
# Also compile against the real public DocumentDB headers:
CONTAINER_RUNTIME=podman PG_MAJOR=18 DOCUMENTDB_SOURCE=../documentdb \
  bash scripts/test-container.sh
# Exercise actual DocumentDB collections (no gateway or host ports needed):
CONTAINER_RUNTIME=podman PG_MAJOR=17 \
  DOCUMENTDB_IMAGE=ghcr.io/documentdb/documentdb/documentdb-local:pg17-0.117.0 \
  bash scripts/test-container.sh
```

The runner creates and removes only its own container, exposes no host ports,
and pins fixture WAL with a physical slot. The real-collection image builds the
module separately, retaining the packaged DocumentDB backend and dependencies.
Build/Python dependencies are installed in containers, not on the host.

The diagnostic accepts a maximum 256 MiB WAL range and a 1-256 MiB allocation
budget. It rejects incomplete/future boundaries and unsupported tuple formats.
Events are identified by commit-record LSN, heap-record LSN, and row ordinal.
This identity is diagnostic, not an authenticated production resume token.

## Limits and remaining work

Fixtures start before all captured writes on a single primary, on one unchanged
heap relation with a stable descriptor and unique, unchanged IDs. BSON IDs must
be stored inline; external IDs are rejected. The fixtures are not
arbitrary history queries. The decoder has no durable schema/collection epoch,
rewrite/drop detection, safe open-transaction replay floor, timeline traversal,
prepared-transaction/recovery validation, or historical image spill. Speculative
inserts and truncation are rejected. Incomplete transactions at the supplied end
are omitted; scanning the original range again after commit includes them.
Memory exhaustion is an explicit error, not a partial successful batch.

Production APIs remain gated on lifecycle/retention correctness, validated
DocumentDB provider hooks and replica-identity ownership, safe starts and
authenticated resumability, recovery/fault tests, and real-driver integration.
Do not enable change streams or depend on this diagnostic in a deployment.
