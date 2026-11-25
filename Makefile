EXTENSION = documentdb_changefeed
MODULE_big = pg_documentdb_changefeed
OBJS = src/extension.o src/wal_decoder.o src/logical_output.o src/logical_provider.o
DATA = sql/documentdb_changefeed--0.1.0.sql
PGFILEDESC = "documentdb_changefeed - experimental logical changefeed provider"

PG_CPPFLAGS = -I$(srcdir)/include $(shell pkg-config --cflags libbson-1.0)
SHLIB_LINK = $(shell pkg-config --libs libbson-1.0) -ldl
PG_CFLAGS = -std=gnu99 -Wall -Wextra -Werror -Wno-unused-parameter \
	-Wno-declaration-after-statement

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

.PHONY: check-documentdb-headers
check-documentdb-headers:
	@test -n "$(DOCUMENTDB_SOURCE)" || \
		{ echo "Set DOCUMENTDB_SOURCE to the public DocumentDB source directory"; exit 1; }
	$(CC) -fsyntax-only $(CPPFLAGS) $(CFLAGS) \
		-I$(DOCUMENTDB_SOURCE)/pg_documentdb_core/include \
		-I$(DOCUMENTDB_SOURCE)/pg_documentdb/include \
		$$(pkg-config --cflags libbson-1.0) $(srcdir)/tests/check_headers.c
