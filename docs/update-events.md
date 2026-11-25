# Update/replacement milestone

## Scope

Build on the logical insert/delete reference using the existing public update
tracker callbacks. Add accurate operator update descriptions and replacement
events without a document-diff classifier, FULL replica identity, or a new core
hook. Pre-images and automatic retention remain separate milestones.

## Why the tracker alone is insufficient

The public tracker is created with the source relation/CTID and reports applied
operator changes before the physical update. Its builder receives the command
classification, not the new document. SQL update-many can also write a no-op
tuple. A heap update by itself therefore cannot prove update versus replacement,
the description, or whether a real logical change happened.

An opt-in AFTER UPDATE row trigger binds the tracker result to the actual old
tuple CTID. It supplies the inserted replacement image from NEW and emits a
transactional logical message. PostgreSQL owns commit, abort, and savepoint
handling for that message.

## Capture policy

Existing three-argument `register_feed` calls keep their insert/delete behavior.
An optional fourth `capture_updates` argument activates the new capture path.
The collection must already have `enableUpdateDescription: true`, which passes
the source relation/CTID through the existing updater. Registration validates
that setting and installs an extension-owned, generation-specific trigger.
Unregister removes only that trigger. Neither operation changes replica identity
or enables logical pre-images.

Capture remains active for registered update feeds when serving is disabled,
so disabling the read gate does not silently create a capture gap.
Unregistered collections retain their normal behavior.

## Transaction-local tracking and decoding

Applied fields/removals are copied through the public BSON writer API. A bounded
transaction-local ledger carries the description and public command type,
indexed by relation and source CTID. Subtransaction abort discards its entries;
release reparents them. The row trigger consumes the matching ledger entry.
An untracked changed heap update is rejected rather than mislabelled.
Untracked physical no-op updates receive an explicit no-op marker.
The ledger permits 4,096 pending rows and 32 MiB of descriptions, with a 16 MiB
per-description limit. Transactional messages are limited to 32 MiB; retained
decoding has separate 64 MiB buffer and output budgets. Exceeding a limit fails
the write or read explicitly.

The logical decoder buffers bounded transaction events in heap-change order,
joins update messages to their rows by relation and typed document key, and emits
events only when the complete transaction is available. No-op markers produce
no event. Missing or ambiguous metadata fails explicitly.

Operator events contain `updateDescription.updatedFields`, `removedFields`,
and `truncatedArrays`; replacement events contain the exact NEW full document.
Pipeline updates follow the public updater's replacement classification.
Same-image replacements still emit replacement events when the public updater
reports a physical replacement; operator no-ops remain suppressed.
Positional updates remain explicitly unsupported in this milestone rather
than synthesize incomplete paths. Ordinary non-positional array operators and
numeric field paths use the applied values reported by the updater.

The supported write surface is the collection API. This reference is not an
authenticated audit log for arbitrary SQL or administrator modifications.
Internal metadata is bounded, private, and not a production token/format
compatibility promise. The shared-slot retention and primary-only limits of
the lower layer remain.

## Validation

Keep all lower-layer suites. Add real collection and authenticated client cases
for operator fields/removals, nested/numeric paths, arrays, replacements,
pipeline classification, same-image replacements, physical no-ops, multiple
updates in one transaction, update-many, abort/savepoints, typed IDs, large
replacement images, replay/page boundaries, unsupported positional mutations,
ordinary unregistered writes, capture while serving is disabled, and recovery
after an oversized batch inside the same transaction.

Build against the pinned public headers; do not copy a different implementation.
The upper PR targets the insert/delete PR's branch.
