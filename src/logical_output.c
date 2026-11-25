/* Copyright (c) Microsoft Corporation.
 * Licensed under the MIT License
 */
#include "postgres.h"

#include <bson.h>
#include "access/htup_details.h"
#include "access/xlog.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "replication/logical.h"
#include "replication/output_plugin.h"
#include "replication/reorderbuffer.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/pg_lsn.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

typedef struct OutputState
{
	Oid relation;
	Oid bsonType;
	XLogRecPtr after;
	int64 ordinal;
	Size bytes;
} OutputState;

void _PG_output_plugin_init(OutputPluginCallbacks *callbacks);

static void
Startup(LogicalDecodingContext *context, OutputPluginOptions *options,
		bool isInit)
{
	OutputState *state = palloc0(sizeof(OutputState));
	ListCell *cell;

	options->output_type = OUTPUT_PLUGIN_BINARY_OUTPUT;
	context->output_plugin_private = state;
	foreach(cell, context->output_plugin_options)
	{
		DefElem *option = lfirst(cell);
		if (strcmp(option->defname, "relation") == 0)
			state->relation = DatumGetObjectId(DirectFunctionCall1(oidin,
												 CStringGetDatum(strVal(option->arg))));
		else if (strcmp(option->defname, "after") == 0)
			state->after = DatumGetLSN(DirectFunctionCall1(pg_lsn_in,
											 CStringGetDatum(strVal(option->arg))));
		else
			ereport(ERROR, (errmsg("unknown changefeed decoding option")));
	}
	if (!isInit && !OidIsValid(state->relation))
		ereport(ERROR, (errmsg("changefeed decoding requires a relation")));
	if (!isInit)
		state->bsonType = GetSysCacheOid2(TYPENAMENSP, Anum_pg_type_oid,
										CStringGetDatum("bson"),
										ObjectIdGetDatum(get_namespace_oid("documentdb_core", false)));
}

static void
Begin(LogicalDecodingContext *context, ReorderBufferTXN *transaction)
{
	OutputState *state = context->output_plugin_private;
	state->ordinal = 0;
}

static void
Commit(LogicalDecodingContext *context, ReorderBufferTXN *transaction,
	   XLogRecPtr commit)
{
}

static bool
TupleBson(HeapTuple tuple, TupleDesc descriptor, Oid bsonType,
		  const char *name, bson_t *value)
{
	for (int index = 0; index < descriptor->natts; index++)
	{
		Form_pg_attribute attribute = TupleDescAttr(descriptor, index);
		bool isNull;
		Datum datum;
		bytea *bytes;

		if (attribute->attisdropped || strcmp(NameStr(attribute->attname), name) != 0)
			continue;
		if (!OidIsValid(bsonType) || attribute->atttypid != bsonType)
			ereport(ERROR, (errmsg("changefeed tuple requires the BSON column type")));
		datum = heap_getattr(tuple, index + 1, descriptor, &isNull);
		if (isNull)
			return false;
		if (VARATT_IS_EXTERNAL_ONDISK(DatumGetPointer(datum)))
			ereport(ERROR, (errmsg("changefeed tuple contains an unreconstructed external value")));
		bytes = DatumGetByteaPP(datum);
		if (!bson_init_static(value, (const uint8_t *) VARDATA_ANY(bytes),
							  VARSIZE_ANY_EXHDR(bytes)) ||
			!bson_validate(value, BSON_VALIDATE_NONE, NULL))
			ereport(ERROR, (errmsg("invalid BSON in changefeed tuple")));
		return true;
	}
	return false;
}

static void
Change(LogicalDecodingContext *context, ReorderBufferTXN *transaction,
	   Relation relation, ReorderBufferChange *change)
{
	OutputState *state = context->output_plugin_private;
	HeapTuple tuple;
	bson_t document;
	bson_t *key;
	bson_t *event;
	bson_iter_t identifier;
	const char *operation;
	char position[32];
	bool insert;

	CHECK_FOR_INTERRUPTS();
	if (RelationGetRelid(relation) != state->relation ||
		transaction->final_lsn < state->after)
		return;
	if (change->action == REORDER_BUFFER_CHANGE_INSERT)
	{
#if PG_VERSION_NUM >= 170000
		tuple = change->data.tp.newtuple;
#else
		tuple = &change->data.tp.newtuple->tuple;
#endif
		operation = "insert";
		insert = true;
	}
	else if (change->action == REORDER_BUFFER_CHANGE_DELETE)
	{
		if (change->data.tp.oldtuple == NULL)
			ereport(ERROR, (errmsg("changefeed delete has no replica identity")));
#if PG_VERSION_NUM >= 170000
		tuple = change->data.tp.oldtuple;
#else
		tuple = &change->data.tp.oldtuple->tuple;
#endif
		operation = "delete";
		insert = false;
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("the reference changefeed does not support updates or truncation")));

	if (insert)
	{
		if (!TupleBson(tuple, RelationGetDescr(relation), state->bsonType, "document", &document) ||
			!bson_iter_init_find(&identifier, &document, "_id"))
			ereport(ERROR, (errmsg("changefeed insert has no document key")));
	}
	else
	{
		if (!TupleBson(tuple, RelationGetDescr(relation), state->bsonType, "object_id", &document) ||
			!bson_iter_init(&identifier, &document) || !bson_iter_next(&identifier))
			ereport(ERROR, (errmsg("changefeed delete has no document key")));
	}
	key = bson_new();
	event = bson_new();
	PG_TRY();
	{
		bson_append_value(key, "_id", -1, bson_iter_value(&identifier));
		snprintf(position, sizeof(position), "%X/%X",
				 LSN_FORMAT_ARGS(transaction->final_lsn));
		BSON_APPEND_UTF8(event, "position", position);
		BSON_APPEND_INT64(event, "ordinal", ++state->ordinal);
		BSON_APPEND_UTF8(event, "operationType", operation);
		BSON_APPEND_DOCUMENT(event, "documentKey", key);
		if (insert)
			BSON_APPEND_DOCUMENT(event, "fullDocument", &document);
		BSON_APPEND_TIMESTAMP(event, "clusterTime",
							  (uint32) timestamptz_to_time_t(transaction->xact_time.commit_time),
							  (uint32) transaction->final_lsn);
		if (event->len > 16 * 1024 * 1024 || state->bytes > 64 * 1024 * 1024 - event->len)
			ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						   errmsg("changefeed decoding output budget exceeded")));
		state->bytes += event->len;
		OutputPluginPrepareWrite(context, true);
		appendBinaryStringInfo(context->out, (const char *) bson_get_data(event), event->len);
		OutputPluginWrite(context, true);
	}
	PG_FINALLY();
	{
		bson_destroy(event);
		bson_destroy(key);
	}
	PG_END_TRY();
}

static void
Truncate(LogicalDecodingContext *context, ReorderBufferTXN *transaction,
		 int relationCount, Relation relations[], ReorderBufferChange *change)
{
	OutputState *state = context->output_plugin_private;
	for (int index = 0; index < relationCount; index++)
		if (RelationGetRelid(relations[index]) == state->relation &&
			transaction->final_lsn >= state->after)
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						   errmsg("the reference changefeed does not support truncation")));
}

void
_PG_output_plugin_init(OutputPluginCallbacks *callbacks)
{
	callbacks->startup_cb = Startup;
	callbacks->begin_cb = Begin;
	callbacks->change_cb = Change;
	callbacks->commit_cb = Commit;
	callbacks->truncate_cb = Truncate;
}
