#include "postgres.h"
#include "utils/builtins.h"

#include "changefeed.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(documentdb_changefeed_version);

Datum
documentdb_changefeed_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text("0.1.0-dev"));
}
