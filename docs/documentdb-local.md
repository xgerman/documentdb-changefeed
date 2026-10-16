# Install into DocumentDB Local

This installs the experimental decoder foundation, **not a working
`mongosh.watch()` implementation**. The extension currently exposes only its
restricted SQL `version()` function. Capture workers, automatic retention,
resume tokens, and the MongoDB provider are not implemented.

Use a disposable container and a fresh data volume. Do not replace libraries
in a running server or reuse a production data directory. PostgreSQL extension
binaries must match the container's PostgreSQL major and CPU architecture.

Commands below run from the `documentdb-changefeed` repository root and use
PostgreSQL 17. Docker, Git, and network access to public build dependencies are
required. The full core build also compiles the Rust gateway and can take
considerably longer than building this module.

## 1. Choose the backend image

For decoder experiments on the stock backend, skip to section 2 with:

```sh
DOCUMENTDB_IMAGE=ghcr.io/documentdb/documentdb/documentdb-local:pg17-0.117.0
```

The stock image does not contain the companion replica-identity hook.

### Include the companion core patch

The patch is [patches/documentdb-changefeed-core.patch](../patches/documentdb-changefeed-core.patch).
It adds an external capture provider's requirement for `REPLICA IDENTITY FULL`
and keeps the logical pre-image setting independent of physical identity.
This extension does **not yet register that hook**; applying the patch alone
does not enable capture or protect a collection automatically.

Build a complete DocumentDB package and Local image from the patched public
source, rather than copying a newly built backend library over a different
release's installed SQL and control files. The source revision below is the
patch's base; it contains DocumentDB 0.118-0.

```sh
CHANGEFEED_DIR="$(pwd)"
git clone https://github.com/documentdb/documentdb.git ../documentdb-changefeed-core
cd ../documentdb-changefeed-core
git checkout --detach d9301892d7fa47b6f5bb923c93b1f6915c3b9bad
git apply --check "$CHANGEFEED_DIR/patches/documentdb-changefeed-core.patch"
git apply "$CHANGEFEED_DIR/patches/documentdb-changefeed-core.patch"

# Build the extension package for Debian 12 / PostgreSQL 17.
./packaging/build_packages.sh --os deb12 --pg 17 \
  --output-dir packages/changefeed

# Select the engine package, not its debug-symbol package.
DEB_PACKAGE_REL_PATH="$(find packages/changefeed -maxdepth 1 \
  -name 'deb12-postgresql-17-documentdb_*.deb' -print)"
test -n "$DEB_PACKAGE_REL_PATH" && test -f "$DEB_PACKAGE_REL_PATH"

# Build the normal gateway image with the matching patched package.
docker build -f packaging/gateway/docker/Dockerfile_documentdb_local \
  --build-arg BASE_IMAGE=debian:bookworm \
  --build-arg POSTGRES_VERSION=17 \
  --build-arg DOCUMENTDB_VERSION=0.118.0 \
  --build-arg DEB_PACKAGE_REL_PATH="$DEB_PACKAGE_REL_PATH" \
  --build-arg GIT_COMMIT="$(git rev-parse HEAD)" \
  -t localhost/documentdb-local-core-changefeed:pg17 .

cd "$CHANGEFEED_DIR"
DOCUMENTDB_IMAGE=localhost/documentdb-local-core-changefeed:pg17
```

Run the commands in order and stop if any command fails. The original clone
is not modified: the patch is applied only to the new sibling clone. For other
source revisions, check applicability and rebuild/test the whole matching
package set; do not assume the patch is release-independent.

## 2. Build the module into the Local image

The `local` target copies only the module, control file, and installation SQL.
It inherits the upstream user, work directory, and gateway entrypoint. Do not
omit `--target local`: the default final stage replaces startup with a private
SQL-fixture server and does not start the gateway.

```sh
docker build -f tests/Dockerfile.documentdb --target local \
  --build-arg PG_MAJOR=17 \
  --build-arg DOCUMENTDB_IMAGE="$DOCUMENTDB_IMAGE" \
  -t localhost/documentdb-local-changefeed:pg17 .
```

The separate builder avoids installing PostgreSQL development packages into
the runtime image, where packaged `libpq` versions can conflict.

## 3. Start a fresh instance and install the SQL extension

These credentials are disposable examples for local testing only. The host
port is bound to loopback; PostgreSQL is not exposed to the host.

```sh
docker run -d --name documentdb-changefeed-local \
  -p 127.0.0.1:10260:10260 \
  -v documentdb-changefeed-local-data:/data \
  localhost/documentdb-local-changefeed:pg17 \
  --username testuser --password testpass123

docker logs -f documentdb-changefeed-local
```

Wait for the gateway's ready message, then stop following logs with Ctrl-C.
This does not stop the container. If port 10260 is occupied, publish
`127.0.0.1:10261:10260` instead.

The image already preloads DocumentDB. Install the new SQL extension as the
backend superuser in the same `postgres` database the gateway uses:

```sh
docker exec --user documentdb documentdb-changefeed-local \
  psql -X -v ON_ERROR_STOP=1 -h /var/run/postgresql -p 9712 \
  -U documentdb -d postgres \
  -c 'CREATE EXTENSION documentdb_changefeed;' \
  -c 'SELECT documentdb_changefeed.version();'
```

The expected module version is `0.1.0-dev`. There is no additional
`shared_preload_libraries` entry required for this foundation.

## 4. Enable logical WAL before decoder experiments

The stock Local entrypoint starts with `wal_level=replica`. The diagnostic
decoder requires `logical`; this setting requires a PostgreSQL restart:

```sh
docker exec --user documentdb documentdb-changefeed-local \
  psql -X -v ON_ERROR_STOP=1 -h /var/run/postgresql -p 9712 \
  -U documentdb -d postgres \
  -c "ALTER SYSTEM SET wal_level = 'logical';"
docker restart documentdb-changefeed-local
docker logs -f documentdb-changefeed-local
```

After the gateway is ready again, stop following logs and verify:

```sh
docker exec --user documentdb documentdb-changefeed-local \
  psql -X -v ON_ERROR_STOP=1 -h /var/run/postgresql -p 9712 \
  -U documentdb -d postgres \
  -c 'SHOW wal_level;' \
  -c 'SELECT extname, extversion FROM pg_extension
      WHERE extname IN (
        $q$documentdb$q$, $q$documentdb_core$q$, $q$documentdb_changefeed$q$
      );'
```

Expect `logical` and all three installed extensions. Logical WAL alone does
not retain history. The private fixtures explicitly pin WAL and set collection
tables to `REPLICA IDENTITY FULL`; neither happens automatically on installation.
Do not create an unattended replication slot: it can retain WAL until disk
fills. Use the bounded, self-cleaning fixture runner documented in the README
for decoding tests.

## 5. Check MongoDB access

Use the `mongosh` shipped inside the image; no host MongoDB client is needed:

```sh
docker exec documentdb-changefeed-local mongosh \
  'mongodb://testuser:testpass123@localhost:10260/?authMechanism=SCRAM-SHA-256&directConnection=true' \
  --quiet --eval '
    const d = db.getSiblingDB("changefeed_install_smoke");
    const assert = require("assert").strict;
    assert.equal(d.items.insertOne({_id: 1, value: 42}).acknowledged, true);
    assert.equal(d.items.findOne({_id: 1}).value, 42);
    assert.equal(d.items.deleteOne({_id: 1}).deletedCount, 1);
    print("MongoDB insert/query/delete OK");
  '
```

This checks gateway access, not change-stream consumption. `watch()` remains
unsupported after installation, including when the core patch is present.
The container-internal MongoDB port stays 10260 even if you publish a different
host port.

## Cleanup and PostgreSQL versions

```sh
docker rm -f documentdb-changefeed-local
# Optional: permanently delete this experiment's data, including its WAL.
docker volume rm documentdb-changefeed-local-data
```

Use Podman in place of Docker for the image build, run, and exec commands. The
upstream package-building script invokes `docker` directly, so that step needs
Docker or a working Podman Docker-compatible CLI configured separately.

PG16/17/18 native decoder fixtures are covered. Real DocumentDB collection
decoding and gateway writes have been exercised on PG17; the core ownership
regression passed against a rebuilt PG17 backend, and its changed translation
units compile on PG18. These results do not validate the full source-packaging
recipe above or PG18 Local runtime integration. To try another major, change
both the source-package/image build and `PG_MAJOR`; never copy a PG17 module
into a PG18 container.
