# Demand-driven collection capture

## Goal

After installing and configuring the extension, an ordinary collection `watch()`
must activate capture without private SQL registration or test provisioning.
Installation alone does not capture collections. Existing manually registered
feeds remain available for diagnostics and keep their administrator-owned slots.

## Activation boundary

A first watch authorizes the caller before asking an extension background worker
to create a logical slot in an independent transaction. The initiating watch
enables the existing update-description source and installs the capture trigger
in its own transaction, where its existing collection locks can be upgraded.
The subscription is acknowledged only after activation commits.
Concurrent first watches serialize on the same collection.

This milestone uses one owned slot per demanded collection, rather than a shared
database slot. That preserves the existing relation-filtered decoder and isolates
cleanup. Slot capacity limits the number of simultaneously retained collections.
Ordinary PostgreSQL WAL still exists for unwatched collections; the plugin does
not enable their update descriptions or install capture triggers.

The first subscription starts from the durable WAL boundary after activation.
Writes before slot creation are outside its history, including transactions
already open before activation. Watching inside a transaction that has performed
writes is rejected, rather than waiting on the caller's own transaction.

## Retention and ownership

`documentdb_changefeed.resume_retention` defaults to `24h`, accepts positive
durations no greater than 24 hours, and is configured through PostgreSQL server
configuration and reload. It bounds both token age and the idle capture grace
period. Each successful poll renews demand, not the age of historical events.

Background maintenance records durable WAL boundaries with observation times.
It advances owned slots past expired boundaries without consuming readers'
unexpired history. An idle feed expires after the configured grace period.
Cleanup removes its trigger and owned slot and restores update descriptions
only when activation enabled them. Manual slots are never advanced or dropped.
Tokens from expired or replaced generations fail explicitly.

Time retention is an upper bound, not a promise that every event remains
available for that duration. The finite PostgreSQL WAL budget and the existing
256 MiB scan limit can make history unavailable sooner. Lowering retention can
expire existing tokens; increasing it does not recover discarded history.

## Integration and limitations

Use the existing availability hook, serving UDF, tailable cursor, and applied
update tracker. There is one provider, no chaining, and no FULL identity
requirement. New installations enable the serving capability by default;
an explicit server setting can disable reads without bypassing cleanup.

PostgreSQL still requires logical WAL, enough worker/slot capacity, the plugin
in the output-library allowlist where applicable, and preloading after the
DocumentDB libraries. Installing SQL cannot remove restart prerequisites.

This milestone supports existing, unsharded collection namespaces. Absent
collections fail without creating them. Database/cluster scopes, DDL events,
pre-images, document lookup modes, positional updates, and durable failover
remain unsupported. Pipeline updates retain the public updater's replacement
classification. Opaque tokens are scope-checked reference tokens, not an
authenticated production format.

The pinned Local gateway returns `whatsmyuri` without the string `you` field
required by the public functional harness's capability probe. Consequently that
harness deselects all four CRUD smoke cases before execution. This is not passing
coverage and must not be bypassed or fixed through private test provisioning.
The insert smoke case also watches an absent namespace, a separate known gap.

## Migration and acceptance

Version 0.2.0 adds ownership, demand timestamps, and maintenance checkpoints.
Upgrade existing installations with `ALTER EXTENSION documentdb_changefeed
UPDATE`; manually registered feeds retain their original ownership semantics.

Exercise ordinary authenticated watch/getMore with no registration or slot
creation in its fixture. Cover CRUD, concurrent activation/readers, resume,
expiry, retained-slot advancement, cleanup, restart, denied activation, and
unchanged unwatched writes. Keep native and manual-feed regression coverage.
