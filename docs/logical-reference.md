# Logical-decoding reference

## Goal and delivery boundary

Provide an experimental, separately installable collection `watch()` provider
using PostgreSQL logical decoding and the existing DocumentDB aggregation UDF.
Do not require a new core hook or the bundled FULL-identity patch.

The first milestone supports inserts and deletes, including typed keys and
large inserted BSON documents. Updates, replacements, pre-images, timestamp
starts, distribution, archive access, and durable failover are explicitly
unsupported. Unsupported mutations must fail rather than disappear or be
mislabelled. Accurate updates require implementing the existing update-tracker
contract; a document diff is not a substitute.

The native-WAL decoder remains an independent feasibility experiment. Its
fixtures and private diagnostic are not the serving implementation.

## Existing integration contract

DocumentDB checks `is_changestream_enabled_and_compatible_hook`, resolves
`documentdb_api_internal.change_stream_aggregation(text, text, bson, bson)`,
and marks the query tailable. The fourth argument carries the cursor state,
including `getpage_batchCount` and `getpage_batchSize`.

The provider returns `(document bson, continuation bson)` rows. Every event
has its own continuation. A final NULL-document row carries progress even
when the batch is empty or all events are filtered. Continuations contain a
raw token in `rt`; the cursor machinery base64-encodes it into
`postBatchResumeToken._data`. Event `_id._data` uses the same encoding.

The provider has one owner and no hook chaining. Preloading its library after
DocumentDB installs the existing availability callback. An explicit,
superuser-controlled `documentdb_changefeed.enable` setting gates serving.
Extension installation does not enable it.

## Logical decoding and retention

The library also implements a binary logical output plugin. PostgreSQL owns
transaction reconstruction, abort/subtransaction handling, and inserted TOAST
reconstruction. The plugin returns typed BSON events rather than diagnostic
text. It filters one exact relation, not a table-name prefix.

An administrator creates a logical slot using this output plugin and registers
that existing slot for one collection. Registration does not create slots,
silently change identity, or own the lifetime of an administrator's slot.
Unregistering the feed does not drop the slot.

Reads peek rather than consume the slot. Independent readers can replay the
same retained history without advancing one another. A per-feed,
transaction-scoped advisory lock serializes decoding; readers can wait behind
another reader's poll or enclosing transaction. This simple milestone has no
automatic slot advancement or retention worker.
The administrator must close unused feeds and drop their slots.

Require a finite positive `max_slot_wal_keep_size`. Reject a read when the
retained scan exceeds 256 MiB, a returned batch exceeds its byte budget, or
decoding exceeds its bounded output budget. A checkpoint-enforced WAL budget
is not an instantaneous disk cap. A lost slot/history produces an error,
not a reset-to-now.
Externally advanced/replaced slots and two-phase slots are rejected.

First subscription starts at the durable current WAL boundary, not at slot
creation. Because the slot retains earlier WAL, transactions that wrote after
slot creation but commit after subscription can still be reconstructed.
The POC does not promise to recover transactions whose writes predate the slot.

Tokens identify the registered slot generation, relation, commit position,
and event ordinal. They are opaque, scope-checked reference tokens, not
authenticated production credentials. Authorization is checked on each read.
Do not accept tokens from the native-WAL diagnostic or another provider.
`clusterTime` uses the commit's timestamp in seconds and the commit LSN's low
word as a reference increment. It is not a production logical-clock contract
and does not support timestamp starts.

## Installation and permissions

Slot administration and feed registration require a PostgreSQL superuser.
The serving UDF runs with its installing owner's privileges for logical-slot
access but must check the invoking role's SELECT privilege on the registered
collection before returning historical images. Plugin metadata is private.
The extension must not overwrite an existing serving UDF or provider.

The initial runtime target is PostgreSQL 17 and the stock DocumentDB Local
image used by the integration tests. Native decoder fixtures retain their
PG16/17/18 matrix. A separate binary is required for each PostgreSQL major.
The integration image updates PostgreSQL within its major to match the current
build headers and adds the BSON runtime dependency. It retains the packaged
DocumentDB backend and gateway. PostgreSQL's output-plugin allowlist must
include this library where that setting exists.

## Acceptance gates

1. Real client `watch()` / `getMore`, not just a diagnostic SQL call.
2. Initial empty/zero-size batch and exclusive per-event resume.
3. Insert/delete keys with default identity, compressed and incompressible
   inserted BSON, aborts, released savepoints followed by parent abort, and
   overlapping commit order.
4. Page boundaries within a transaction, filtered batches, two independent
   readers, restart/replay while the same slot survives, and explicit history
   and resource-limit errors.
5. Unauthorized reads, cross-collection tokens, disabled-provider behavior,
   unregister/slot cleanup, and unchanged ordinary collection operations.

Update-description support and automatic retention are subsequent milestones,
not implied by this proof of the existing provider seam.
