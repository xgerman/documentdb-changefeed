# DocumentDB Changefeed

An experimental, separately installable collection `watch()` provider using
PostgreSQL logical decoding and DocumentDB's existing aggregation/cursor UDF.
The reference supports **inserts and deletes with default replica identity**,
plus operator updates and replacements, per-event resume, independent
readers, and filtered batches.
It does not require the companion core patch or hook chaining.

**Not production-ready.** Positional updates, pre-images, timestamp starts,
distribution, absent-namespace watches, archives, and durable failover are
unsupported. Ordinary `watch()` activates capture only on the requested existing
collection, with an owned slot and automatic bounded retention.
Unsupported mutations fail explicitly rather than disappear.

See [the design and delivery boundary](docs/logical-reference.md).
See [the update/replacement design](docs/update-events.md) for capture policy,
applied update descriptions, and transactional logical messages.
See [demand activation and retention](docs/demand-capture.md) for lifecycle,
ownership, migration, and explicit feature gaps.
The old native-WAL decoder remains an independent experiment, not the serving
implementation. PostgreSQL 16/17/18 native fixtures are retained; the logical
provider and real client integration are validated on PostgreSQL 17.

## Install into DocumentDB Local

See [the container installation guide](docs/documentdb-local.md). The extension
must be preloaded after DocumentDB in each serving backend. With the required
server settings in place, installation makes `watch()` available without feed
registration. No collection is captured until requested.

`documentdb_changefeed.resume_retention` defaults to `24h` and can be shortened
through server configuration and reload. It bounds history age and the idle
capture grace period. Resource limits can expire history sooner.

Run the complete reference proof in a disposable container, with no host ports:

```sh
CONTAINER_RUNTIME=podman PG_MAJOR=17 \
  DOCUMENTDB_IMAGE=ghcr.io/documentdb/documentdb/documentdb-local:pg17-0.117.0 \
  CHANGEFEED_LOGICAL_TEST=1 bash scripts/test-container.sh
```

This runs the existing native collection fixtures, logical insert/delete
and update/replacement regressions, schema upgrade, authenticated client
`watch()`/`getMore` without registration, demand/expiry/cleanup regressions,
and retained-token replay after a primary restart.
Use `CONTAINER_RUNTIME=docker` for Docker.
The integration image preserves the packaged DocumentDB backend and gateway,
installs the BSON runtime library, and updates PostgreSQL within its major to
match the current development headers.

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
faked. These native-decoder suites are separate from the logical `watch()` proof.

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

The logical reference does not use this diagnostic or inherit its token format.
Production readiness still requires broader lifecycle coverage,
positional update support, authenticated tokens and migration policy, additional
failure/resource tests, and broader backend/gateway compatibility. The bundled
replica-identity patch is a deferred storage-policy experiment, not a prerequisite
for the logical reference.
