#ifndef DOCUMENTDB_CHANGEFEED_H
#define DOCUMENTDB_CHANGEFEED_H

#include "postgres.h"
#include "fmgr.h"

#if PG_VERSION_NUM < 160000 || PG_VERSION_NUM >= 190000
#error "documentdb_changefeed supports PostgreSQL 16, 17 and 18"
#endif

extern PGDLLEXPORT Datum documentdb_changefeed_version(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum documentdb_changefeed_decode_test(PG_FUNCTION_ARGS);

#endif
