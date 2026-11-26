/* Copyright (c) Microsoft Corporation.
 * Licensed under the MIT License
 */
#include "postgres.h"

#include <bson.h>
#include "access/htup_details.h"
#include "access/xlog.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "common/hashfn.h"
#include "miscadmin.h"
#include "replication/logical.h"
#include "replication/output_plugin.h"
#include "replication/reorderbuffer.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

typedef struct PendingEvent
{
	uint8_t *data;
	uint32_t length;
	uint8_t *metadata;
	uint32_t metadataLength;
	bool update;
	bool noop;
} PendingEvent;

typedef struct EventQueue
{
	uint64 hash;
	List *events;
} EventQueue;

typedef struct OutputState
{
	Oid relation;
	Oid bsonType;
	XLogRecPtr after;
	int64 ordinal;
	Size bytes;
	Size outputBytes;
	char *generation;
	MemoryContext transactionMemory;
	List *events;
	HTAB *updates;
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
	state->transactionMemory = AllocSetContextCreate(context->context,
													"changefeed transaction events",
													ALLOCSET_DEFAULT_SIZES);
	foreach(cell, context->output_plugin_options)
	{
		DefElem *option = lfirst(cell);
		if (strcmp(option->defname, "relation") == 0)
			state->relation = DatumGetObjectId(DirectFunctionCall1(oidin,
												 CStringGetDatum(strVal(option->arg))));
		else if (strcmp(option->defname, "after") == 0)
			state->after = DatumGetLSN(DirectFunctionCall1(pg_lsn_in,
											 CStringGetDatum(strVal(option->arg))));
		else if (strcmp(option->defname, "generation") == 0)
			state->generation = pstrdup(strVal(option->arg));
		else
			ereport(ERROR, (errmsg("unknown changefeed decoding option")));
	}
	if (!isInit && !context->fast_forward && !OidIsValid(state->relation))
		ereport(ERROR, (errmsg("changefeed decoding requires a relation")));
	if (!isInit && !context->fast_forward)
		state->bsonType = GetSysCacheOid2(TYPENAMENSP, Anum_pg_type_oid,
										CStringGetDatum("bson"),
										ObjectIdGetDatum(get_namespace_oid("documentdb_core", false)));
}

static void
Begin(LogicalDecodingContext *context, ReorderBufferTXN *transaction)
{
	OutputState *state = context->output_plugin_private;
	MemoryContextReset(state->transactionMemory);
	state->events = NIL;
	state->updates = NULL;
	state->ordinal = 0;
}

static const uint8_t *
DocumentKey(const bson_t *event, uint32_t *length)
{
	bson_iter_t iterator;
	const uint8_t *data;
	if (!bson_iter_init_find(&iterator, event, "documentKey") ||
		!BSON_ITER_HOLDS_DOCUMENT(&iterator))
		ereport(ERROR, (errmsg("changefeed event has no typed document key")));
	bson_iter_document(&iterator, length, &data);
	return data;
}

static void
StoreEvent(OutputState *state, const bson_t *event, bool update)
{
	MemoryContext saved;
	PendingEvent *pending;
	if (event->len > 16 * 1024 * 1024 ||
		state->bytes > 64 * 1024 * 1024 - event->len)
		ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					   errmsg("changefeed decoding buffer budget exceeded")));
	state->bytes += event->len;
	saved = MemoryContextSwitchTo(state->transactionMemory);
	pending = palloc0(sizeof(*pending));
	pending->data = palloc(event->len);
	memcpy(pending->data, bson_get_data(event), event->len);
	pending->length = event->len;
	pending->update = update;
	state->events = lappend(state->events, pending);
	if (update)
	{
		uint32_t length;
		const uint8_t *key = DocumentKey(event, &length);
		uint64 hash = hash_bytes_extended(key, length, 0);
		EventQueue *queue;
		bool found;
		if (state->updates == NULL)
		{
			HASHCTL control = {0};
			control.keysize = sizeof(uint64);
			control.entrysize = sizeof(EventQueue);
			control.hcxt = state->transactionMemory;
			state->updates = hash_create("changefeed pending update keys", 32,
										 &control, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
		}
		queue = hash_search(state->updates, &hash, HASH_ENTER, &found);
		if (!found)
			queue->events = NIL;
		queue->events = lappend(queue->events, pending);
	}
	MemoryContextSwitchTo(saved);
}

static void
Message(LogicalDecodingContext *context, ReorderBufferTXN *transaction,
		XLogRecPtr lsn, bool transactional, const char *prefix,
		Size length, const char *data)
{
	OutputState *state = context->output_plugin_private;
	bson_t metadata;
	bson_iter_t iterator;
	const uint8_t *key;
	uint32_t keyLength;
	uint64 hash;
	EventQueue *queue;
	PendingEvent *matched = NULL;
	ListCell *cell;
	const char *operation;

	if (!transactional || transaction == NULL ||
		strcmp(prefix, "documentdb_changefeed.update.v1") != 0 ||
		transaction->final_lsn < state->after)
		return;
	if (length > 32 * 1024 * 1024 ||
		!bson_init_static(&metadata, (const uint8_t *) data, length) ||
		!bson_validate(&metadata, BSON_VALIDATE_NONE, NULL))
		ereport(ERROR, (errmsg("invalid changefeed update metadata")));
	if (!bson_iter_init_find(&iterator, &metadata, "relation") ||
		!BSON_ITER_HOLDS_INT64(&iterator))
		ereport(ERROR, (errmsg("update metadata requires a relation")));
	if (bson_iter_int64(&iterator) != state->relation)
		return;
	if (!bson_iter_init_find(&iterator, &metadata, "generation") ||
		!BSON_ITER_HOLDS_UTF8(&iterator) || state->generation == NULL ||
		strcmp(bson_iter_utf8(&iterator, NULL), state->generation) != 0)
		ereport(ERROR, (errmsg("changefeed update generation does not match")));
	key = DocumentKey(&metadata, &keyLength);
	hash = hash_bytes_extended(key, keyLength, 0);
	queue = state->updates == NULL ? NULL : hash_search(state->updates, &hash, HASH_FIND, NULL);
	if (queue != NULL)
		foreach(cell, queue->events)
		{
			PendingEvent *pending = lfirst(cell);
			bson_t event;
			uint32_t candidateLength;
			const uint8_t *candidate;
			bson_init_static(&event, pending->data, pending->length);
			candidate = DocumentKey(&event, &candidateLength);
			if (pending->metadata == NULL && candidateLength == keyLength &&
				memcmp(candidate, key, keyLength) == 0)
			{
				matched = pending;
				break;
			}
		}
	if (matched == NULL)
		ereport(ERROR, (errmsg("update metadata has no matching heap mutation")));
	if (!bson_iter_init_find(&iterator, &metadata, "operationType") ||
		!BSON_ITER_HOLDS_UTF8(&iterator))
		ereport(ERROR, (errmsg("update metadata requires an operation type")));
	operation = bson_iter_utf8(&iterator, NULL);
	if (strcmp(operation, "noop") != 0 &&
		strcmp(operation, "update") != 0 && strcmp(operation, "replace") != 0)
		ereport(ERROR, (errmsg("invalid tracked update operation")));
	if (state->bytes > 64 * 1024 * 1024 - length)
		ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					   errmsg("changefeed update metadata budget exceeded")));
	state->bytes += length;
	matched->metadata = MemoryContextAlloc(state->transactionMemory, length);
	memcpy(matched->metadata, data, length);
	matched->metadataLength = length;
	matched->noop = strcmp(operation, "noop") == 0;
	queue->events = list_delete_ptr(queue->events, matched);
}

static void
Commit(LogicalDecodingContext *context, ReorderBufferTXN *transaction,
	   XLogRecPtr commit)
{
	OutputState *state = context->output_plugin_private;
	ListCell *cell;
	bson_t *volatile event = NULL;
	PG_TRY();
	{
		foreach(cell, state->events)
		{
			PendingEvent *pending = lfirst(cell);
			bson_t stored;
			if (pending->update && pending->metadata == NULL)
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							   errmsg("changefeed update has no capture metadata")));
			if (pending->noop)
				continue;
			bson_init_static(&stored, pending->data, pending->length);
			event = bson_copy(&stored);
			BSON_APPEND_DATE_TIME(event, "wallTime",
								  transaction->xact_time.commit_time / 1000 +
								  (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * INT64CONST(86400000));
			if (pending->update)
			{
				bson_t metadata;
				bson_iter_t iterator;
				bson_init_static(&metadata, pending->metadata, pending->metadataLength);
				bson_iter_init(&iterator, &metadata);
				while (bson_iter_next(&iterator))
				{
					const char *name = bson_iter_key(&iterator);
					if (strcmp(name, "operationType") == 0 ||
						strcmp(name, "fullDocument") == 0 ||
						strcmp(name, "updateDescription") == 0)
						bson_append_value(event, name, -1, bson_iter_value(&iterator));
				}
			}
			if (event->len > 16 * 1024 * 1024 ||
				state->outputBytes > 64 * 1024 * 1024 - event->len)
				ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
							   errmsg("changefeed decoding output budget exceeded")));
			state->outputBytes += event->len;
			OutputPluginPrepareWrite(context, true);
			appendBinaryStringInfo(context->out, (const char *) bson_get_data(event), event->len);
			OutputPluginWrite(context, true);
			bson_destroy(event);
			event = NULL;
		}
	}
	PG_CATCH();
	{
		if (event != NULL)
			bson_destroy(event);
		PG_RE_THROW();
	}
	PG_END_TRY();
	MemoryContextReset(state->transactionMemory);
	state->events = NIL;
	state->updates = NULL;
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
	volatile bool update = false;

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
	else if (change->action == REORDER_BUFFER_CHANGE_UPDATE)
	{
#if PG_VERSION_NUM >= 170000
		tuple = change->data.tp.newtuple;
#else
		tuple = &change->data.tp.newtuple->tuple;
#endif
		insert = false;
		update = true;
		operation = "update";
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
		if (!update)
			BSON_APPEND_UTF8(event, "operationType", operation);
		BSON_APPEND_DOCUMENT(event, "documentKey", key);
		if (insert)
			BSON_APPEND_DOCUMENT(event, "fullDocument", &document);
		BSON_APPEND_TIMESTAMP(event, "clusterTime",
							  (uint32) timestamptz_to_time_t(transaction->xact_time.commit_time),
							  (uint32) transaction->final_lsn);
		StoreEvent(state, event, update);
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
	callbacks->message_cb = Message;
}
