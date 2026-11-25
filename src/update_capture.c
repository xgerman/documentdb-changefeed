/* Copyright (c) Microsoft Corporation.
 * Licensed under the MIT License
 */
#include "postgres.h"

#include <dlfcn.h>
#include "access/htup_details.h"
#include "access/skey.h"
#include "access/stratnum.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "commands/trigger.h"
#include "executor/tuptable.h"
#include "replication/message.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include "update/update_hooks.h"
#include "update/bson_update_common.h"
#include "changefeed.h"

PG_FUNCTION_INFO_V1(documentdb_changefeed_capture_update);

typedef struct UpdateKey
{
	Oid relation;
	ItemPointerData tid;
} UpdateKey;

typedef struct UpdateEntry
{
	UpdateKey key;
	SubTransactionId subid;
	char *generation;
	CommandUpdateType type;
	pgbson *description;
} UpdateEntry;

struct BsonUpdateTracker
{
	UpdateKey key;
	char *generation;
	pgbson_writer updated;
	List *removed;
	bool positional;
};

static struct
{
	void (*init)(pgbson_writer *);
	void (*appendValue)(pgbson_writer *, const char *, uint32_t, const bson_value_t *);
	void (*appendDocument)(pgbson_writer *, const char *, uint32_t, const pgbson *);
	void (*appendInt64)(pgbson_writer *, const char *, uint32_t, int64);
	void (*appendUtf8)(pgbson_writer *, const char *, uint32_t, const char *);
	void (*emptyArray)(pgbson_writer *, const char *, uint32_t);
	void (*startArray)(pgbson_writer *, const char *, uint32_t, pgbson_array_writer *);
	void (*endArray)(pgbson_writer *, pgbson_array_writer *);
	void (*arrayUtf8)(pgbson_array_writer *, const char *);
	pgbson *(*get)(pgbson_writer *);
	pgbson *(*clone)(const pgbson *);
	void (*iterator)(const pgbson *, bson_iter_t *);
} Core;

static CreateBsonUpdateTracker_HookType *CreateHook;
static BuildUpdateDescription_HookType *BuildHook;
static NotifyUpdatedField_HookType *UpdatedHook;
static NotifyRemovedField_HookType *RemovedHook;
static StartPositionalUpdate_HookType *PositionalHook;
static HTAB *Updates;
static Size UpdateBytes;

static void *
Symbol(const char *name)
{
	void *symbol = dlsym(RTLD_DEFAULT, name);
	if (symbol == NULL)
		ereport(ERROR, (errmsg("required update capture symbol is unavailable: %s", name)));
	return symbol;
}

static char *
CaptureGeneration(Oid source)
{
	Oid schema;
	Oid oid;
	Relation feeds;
	TableScanDesc scan;
	TupleTableSlot *slot;
	ScanKeyData key;
	char *generation = NULL;

	if (!OidIsValid(get_extension_oid("documentdb_changefeed", true)))
		return NULL;
	schema = get_namespace_oid("documentdb_changefeed", false);
	oid = get_relname_relid("feeds", schema);
	feeds = table_open(oid, AccessShareLock);
	slot = table_slot_create(feeds, NULL);
	ScanKeyInit(&key, 4, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(source));
	scan = table_beginscan(feeds, GetActiveSnapshot(), 1, &key);
	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		bool isNull;
		Datum enabled = slot_getattr(slot, 8, &isNull);
		if (!isNull && DatumGetBool(enabled))
		{
			Datum value = slot_getattr(slot, 6, &isNull);
			if (!isNull)
				generation = TextDatumGetCString(value);
		}
		break;
	}
	table_endscan(scan);
	ExecDropSingleTupleTableSlot(slot);
	table_close(feeds, AccessShareLock);
	return generation;
}

static BsonUpdateTracker *
CreateTracker(BsonUpdateSource *source)
{
	BsonUpdateTracker *tracker;
	char *generation;
	if (!BsonUpdateSourceIsValid(source))
		return NULL;
	generation = CaptureGeneration(source->tableOid);
	if (generation == NULL)
		return NULL;
	tracker = MemoryContextAllocAligned(CurrentMemoryContext, sizeof(*tracker),
									   128, MCXT_ALLOC_ZERO);
	tracker->key.relation = source->tableOid;
	tracker->key.tid = *source->ctid;
	tracker->generation = generation;
	Core.init(&tracker->updated);
	return tracker;
}

bool
UpdateCaptureAvailable(void)
{
	return CreateHook != NULL && *CreateHook == CreateTracker;
}

static void
Updated(BsonUpdateTracker *tracker, const char *path, const bson_value_t *value)
{
	if (tracker != NULL)
		Core.appendValue(&tracker->updated, path, -1, value);
}

static void
Removed(BsonUpdateTracker *tracker, const char *path)
{
	if (tracker != NULL)
		tracker->removed = lappend(tracker->removed, pstrdup(path));
}

static void
Positional(BsonUpdateTracker *tracker, uint32 type, bool root,
		   const StringView *relative, const StringView *path)
{
	if (tracker != NULL)
		tracker->positional = true;
}

static void
Transaction(XactEvent event, void *argument)
{
	if (event == XACT_EVENT_COMMIT || event == XACT_EVENT_ABORT ||
		event == XACT_EVENT_PREPARE)
	{
		Updates = NULL;
		UpdateBytes = 0;
	}
}

static void
Subtransaction(SubXactEvent event, SubTransactionId subid,
			   SubTransactionId parent, void *argument)
{
	HASH_SEQ_STATUS sequence;
	UpdateEntry *entry;
	if (Updates == NULL)
		return;
	hash_seq_init(&sequence, Updates);
	while ((entry = hash_seq_search(&sequence)) != NULL)
	{
		if (entry->subid != subid)
			continue;
		if (event == SUBXACT_EVENT_COMMIT_SUB)
			entry->subid = parent;
		else if (event == SUBXACT_EVENT_ABORT_SUB)
		{
			UpdateBytes -= VARSIZE(entry->description);
			pfree(entry->description);
			pfree(entry->generation);
			hash_search(Updates, &entry->key, HASH_REMOVE, NULL);
		}
	}
}

static pgbson *
Build(BsonUpdateTracker *tracker, CommandUpdateType type)
{
	pgbson_writer writer;
	pgbson_array_writer removed;
	pgbson *updated;
	pgbson *description;
	pgbson *copy;
	char *generation;
	SubTransactionId subid;
	MemoryContext saved;
	UpdateEntry *entry;
	bool found;
	ListCell *cell;

	if (tracker == NULL)
		return NULL;
	if (type != CommandUpdateType_Update && type != CommandUpdateType_Replacement)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					   errmsg("unsupported reference update classification")));
	if (tracker->positional)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					   errmsg("reference update capture does not support positional updates")));
	Core.init(&writer);
	updated = Core.get(&tracker->updated);
	Core.appendDocument(&writer, "updatedFields", -1, updated);
	Core.startArray(&writer, "removedFields", -1, &removed);
	foreach(cell, tracker->removed)
		Core.arrayUtf8(&removed, lfirst(cell));
	Core.endArray(&writer, &removed);
	Core.emptyArray(&writer, "truncatedArrays", -1);
	description = Core.get(&writer);
	if (VARSIZE(description) > 16 * 1024 * 1024 ||
		UpdateBytes + VARSIZE(description) > 32 * 1024 * 1024)
		ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					   errmsg("reference update tracking budget exceeded")));
	saved = MemoryContextSwitchTo(TopTransactionContext);
	if (Updates == NULL)
	{
		HASHCTL control = {0};
		control.keysize = sizeof(UpdateKey);
		control.entrysize = sizeof(UpdateEntry);
		control.hcxt = TopTransactionContext;
		Updates = hash_create("changefeed update ledger", 32, &control,
							  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}
	if (hash_get_num_entries(Updates) >= 4096)
		ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					   errmsg("oversized reference update ledger")));
	copy = Core.clone(description);
	generation = pstrdup(tracker->generation);
	subid = GetCurrentSubTransactionId();
	entry = hash_search(Updates, &tracker->key, HASH_ENTER, &found);
	if (found)
		ereport(ERROR, (errmsg("ambiguous reference update ledger")));
	entry->subid = subid;
	entry->generation = generation;
	entry->type = type;
	entry->description = copy;
	UpdateBytes += VARSIZE(entry->description);
	MemoryContextSwitchTo(saved);
	return description;
}

static pgbson *
TupleValue(HeapTuple tuple, Relation relation, const char *name)
{
	TupleDesc descriptor = RelationGetDescr(relation);
	Oid bsonType = GetSysCacheOid2(TYPENAMENSP, Anum_pg_type_oid,
								  CStringGetDatum("bson"),
								  ObjectIdGetDatum(get_namespace_oid("documentdb_core", false)));
	for (int index = 0; index < descriptor->natts; index++)
	{
		Form_pg_attribute attribute = TupleDescAttr(descriptor, index);
		if (!attribute->attisdropped && strcmp(NameStr(attribute->attname), name) == 0)
		{
			bool isNull;
			if (!OidIsValid(bsonType) || attribute->atttypid != bsonType)
				ereport(ERROR, (errmsg("reference capture requires the BSON column type")));
			Datum value = heap_getattr(tuple, index + 1, descriptor, &isNull);
			if (!isNull)
				return DatumGetPgBson(value);
		}
	}
	ereport(ERROR, (errmsg("reference capture requires a non-null BSON column: %s", name)));
	return NULL;
}

Datum
documentdb_changefeed_capture_update(PG_FUNCTION_ARGS)
{
	TriggerData *trigger;
	UpdateKey key = {0};
	UpdateEntry *entry;
	pgbson *oldDocument;
	pgbson *newDocument;
	pgbson *identifier;
	bson_iter_t iterator;
	pgbson_writer writer;
	pgbson_writer documentKey;
	pgbson *keyDocument;
	pgbson *message;

	if (!CALLED_AS_TRIGGER(fcinfo))
		ereport(ERROR, (errmsg("capture_update must be invoked as a row trigger")));
	if (!UpdateCaptureAvailable())
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					   errmsg("update capture requires preloading after DocumentDB")));
	trigger = (TriggerData *) fcinfo->context;
	if (!TRIGGER_FIRED_AFTER(trigger->tg_event) ||
		!TRIGGER_FIRED_FOR_ROW(trigger->tg_event) ||
		!TRIGGER_FIRED_BY_UPDATE(trigger->tg_event) ||
		trigger->tg_trigger->tgnargs != 1)
		ereport(ERROR, (errmsg("invalid reference update trigger")));
	key.relation = RelationGetRelid(trigger->tg_relation);
	key.tid = trigger->tg_trigtuple->t_self;
	entry = Updates == NULL ? NULL : hash_search(Updates, &key, HASH_FIND, NULL);
	oldDocument = TupleValue(trigger->tg_trigtuple, trigger->tg_relation, "document");
	newDocument = TupleValue(trigger->tg_newtuple, trigger->tg_relation, "document");
	if (entry == NULL &&
		(VARSIZE(oldDocument) != VARSIZE(newDocument) ||
		 memcmp(VARDATA(oldDocument), VARDATA(newDocument), VARSIZE_ANY_EXHDR(oldDocument)) != 0))
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					   errmsg("reference update capture requires tracked collection API writes")));
	if (entry != NULL &&
		strcmp(entry->generation, trigger->tg_trigger->tgargs[0]) != 0)
		ereport(ERROR, (errmsg("reference update capture generation changed")));
	identifier = TupleValue(trigger->tg_newtuple, trigger->tg_relation, "object_id");
	Core.iterator(identifier, &iterator);
	if (!bson_iter_next(&iterator))
		ereport(ERROR, (errmsg("reference update capture requires a document key")));
	Core.init(&documentKey);
	Core.appendValue(&documentKey, "_id", -1, bson_iter_value(&iterator));
	keyDocument = Core.get(&documentKey);
	Core.init(&writer);
	Core.appendInt64(&writer, "relation", -1, key.relation);
	Core.appendUtf8(&writer, "generation", -1, trigger->tg_trigger->tgargs[0]);
	Core.appendDocument(&writer, "documentKey", -1, keyDocument);
	Core.appendUtf8(&writer, "operationType", -1,
				   entry == NULL ? "noop" :
				   entry->type == CommandUpdateType_Replacement ? "replace" : "update");
	if (entry != NULL)
	{
		if (entry->type == CommandUpdateType_Replacement)
			Core.appendDocument(&writer, "fullDocument", -1, newDocument);
		else
			Core.appendDocument(&writer, "updateDescription", -1, entry->description);
	}
	message = Core.get(&writer);
	if (VARSIZE_ANY_EXHDR(message) > 32 * 1024 * 1024)
		ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					   errmsg("reference update message exceeds its size limit")));
	LogLogicalMessage("documentdb_changefeed.update.v1",
					  VARDATA_ANY(message), VARSIZE_ANY_EXHDR(message), true
#if PG_VERSION_NUM >= 170000
					  , false
#endif
					  );
	if (entry != NULL)
	{
		UpdateBytes -= VARSIZE(entry->description);
		pfree(entry->description);
		pfree(entry->generation);
		hash_search(Updates, &key, HASH_REMOVE, NULL);
	}
	PG_RETURN_POINTER(trigger->tg_newtuple);
}

void
InitializeUpdateCapture(void)
{
	CreateHook = dlsym(RTLD_DEFAULT, "create_update_tracker_hook");
	if (CreateHook == NULL)
		return;
	BuildHook = Symbol("build_update_description_hook");
	UpdatedHook = Symbol("notify_updated_field_hook");
	RemovedHook = Symbol("notify_remove_field_hook");
	PositionalHook = Symbol("start_positional_update_hook");
	if (*CreateHook || *BuildHook || *UpdatedHook || *RemovedHook || *PositionalHook ||
		*(NotifyPositionalMatchIndex_HookType *) Symbol("notify_positional_match_index_hook") ||
		*(RemoveLastMatchedIndex_HookType *) Symbol("remove_last_matched_index_hook") ||
		*(EndPositionalUpdate_HookType *) Symbol("end_positional_update_hook"))
		ereport(ERROR, (errmsg("an update tracker is already registered")));
#define LOAD(member, name) Core.member = Symbol(name)
	LOAD(init, "PgbsonWriterInit");
	LOAD(appendValue, "PgbsonWriterAppendValue");
	LOAD(appendDocument, "PgbsonWriterAppendDocument");
	LOAD(appendInt64, "PgbsonWriterAppendInt64");
	LOAD(appendUtf8, "PgbsonWriterAppendUtf8");
	LOAD(emptyArray, "PgbsonWriterAppendEmptyArray");
	LOAD(startArray, "PgbsonWriterStartArray");
	LOAD(endArray, "PgbsonWriterEndArray");
	LOAD(arrayUtf8, "PgbsonArrayWriterWriteUtf8");
	LOAD(get, "PgbsonWriterGetPgbson");
	LOAD(clone, "PgbsonCloneFromPgbson");
	LOAD(iterator, "PgbsonInitIterator");
#undef LOAD
	*CreateHook = CreateTracker;
	*BuildHook = Build;
	*UpdatedHook = Updated;
	*RemovedHook = Removed;
	*PositionalHook = Positional;
	RegisterXactCallback(Transaction, NULL);
	RegisterSubXactCallback(Subtransaction, NULL);
}

void
FinalizeUpdateCapture(void)
{
	if (CreateHook == NULL)
		return;
	if (*CreateHook == CreateTracker) *CreateHook = NULL;
	if (*BuildHook == Build) *BuildHook = NULL;
	if (*UpdatedHook == Updated) *UpdatedHook = NULL;
	if (*RemovedHook == Removed) *RemovedHook = NULL;
	if (*PositionalHook == Positional) *PositionalHook = NULL;
	UnregisterXactCallback(Transaction, NULL);
	UnregisterSubXactCallback(Subtransaction, NULL);
}
