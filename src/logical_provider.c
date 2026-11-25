/* Copyright (c) Microsoft Corporation.
 * Licensed under the MIT License
 */
#include "postgres.h"

#include <bson.h>
#include "access/xlog.h"
#include "catalog/namespace.h"
#include "common/base64.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/pg_lsn.h"

#include "changefeed.h"

PG_FUNCTION_INFO_V1(documentdb_changefeed_register);
PG_FUNCTION_INFO_V1(documentdb_changefeed_unregister);
PG_FUNCTION_INFO_V1(documentdb_changefeed_aggregate);

#if PG_VERSION_NUM >= 180000
typedef uint8 Base64Byte;
#else
typedef char Base64Byte;
#endif

typedef struct Feed
{
	Oid relation;
	char *slot;
	char *generation;
	XLogRecPtr start;
} Feed;

static void
Execute(const char *query, int count, Oid *types, Datum *values, bool readOnly)
{
	int result = SPI_execute_with_args(query, count, types, values, NULL,
									   readOnly, 0);
	if (result < 0)
		ereport(ERROR, (errmsg("changefeed metadata query failed")));
}

static Datum
Column(int index)
{
	bool isNull;
	Datum value = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc,
							   index, &isNull);
	if (isNull)
		ereport(ERROR, (errmsg("changefeed metadata is incomplete")));
	return value;
}

static void
InitBson(Datum datum, bson_t *value)
{
	bytea *bytes = DatumGetByteaPP(datum);
	if (!bson_init_static(value, (const uint8_t *) VARDATA_ANY(bytes),
						  VARSIZE_ANY_EXHDR(bytes)) ||
		!bson_validate(value, BSON_VALIDATE_NONE, NULL))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					   errmsg("invalid changefeed BSON")));
}

static bytea *
CopyBson(const bson_t *value)
{
	bytea *result = palloc(VARHDRSZ + value->len);
	SET_VARSIZE(result, VARHDRSZ + value->len);
	memcpy(VARDATA(result), bson_get_data(value), value->len);
	return result;
}

static void
ValidateNamespace(text *value)
{
	if (VARSIZE_ANY_EXHDR(value) == 0 ||
		memchr(VARDATA_ANY(value), '\0', VARSIZE_ANY_EXHDR(value)) != NULL)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					   errmsg("changefeed namespace must be nonempty without embedded nulls")));
}

static void
CheckRetention(void)
{
	if (SPI_execute("SELECT pg_size_bytes(current_setting('max_slot_wal_keep_size')) > 0",
					true, 1) != SPI_OK_SELECT || !DatumGetBool(Column(1)))
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					   errmsg("changefeed requires a finite positive max_slot_wal_keep_size")));
}

Datum
documentdb_changefeed_register(PG_FUNCTION_ARGS)
{
	text *database = PG_GETARG_TEXT_PP(0);
	text *collection = PG_GETARG_TEXT_PP(1);
	text *slot = PG_GETARG_TEXT_PP(2);
	Oid types[] = {TEXTOID, TEXTOID, TEXTOID};
	Datum values[] = {PointerGetDatum(database), PointerGetDatum(collection),
					  PointerGetDatum(slot)};
	Oid relation;
	char *generation;
	uint8 random[16];
	char hex[33];
	XLogRecPtr start;
	bool isNull;
	Datum shard;
	bool captureUpdates = PG_GETARG_BOOL(3);

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					   errmsg("changefeed registration requires superuser")));
	ValidateNamespace(database);
	ValidateNamespace(collection);
	ValidateNamespace(slot);
	if (captureUpdates && !UpdateCaptureAvailable())
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					   errmsg("update capture requires preloading after DocumentDB")));
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "could not connect to SPI");
	CheckRetention();
	Execute("SELECT ('documentdb_data.documents_' || collection_id)::regclass::oid, shard_key,options"
			" FROM documentdb_api_catalog.collections"
			" WHERE database_name=$1 AND collection_name=$2 AND view_definition IS NULL",
			2, types, values, true);
	if (SPI_processed != 1)
		ereport(ERROR, (errmsg("changefeed requires an existing collection")));
	relation = DatumGetObjectId(Column(1));
	shard = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isNull);
	(void) shard;
	if (!isNull)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					   errmsg("the reference changefeed does not support sharded collections")));
	if (captureUpdates)
	{
		bson_t options;
		bson_iter_t setting;
		Datum value = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3, &isNull);
		bool enabled = false;
		if (!isNull)
		{
			InitBson(value, &options);
			enabled = bson_iter_init_find(&setting, &options, "enableUpdateDescription") &&
				BSON_ITER_HOLDS_BOOL(&setting) && bson_iter_bool(&setting);
		}
		if (!enabled)
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						   errmsg("update capture requires enableUpdateDescription on the collection")));
	}
	Execute("SELECT confirmed_flush_lsn FROM pg_replication_slots"
			" WHERE slot_name=$1 AND database=current_database()"
			" AND plugin='pg_documentdb_changefeed' AND NOT active AND NOT two_phase"
			" AND wal_status IN ('reserved', 'extended')",
			1, types, &values[2], true);
	if (SPI_processed != 1)
		ereport(ERROR, (errmsg("changefeed requires an available logical slot using its output plugin")));
	start = DatumGetLSN(Column(1));
	if (!pg_strong_random(random, sizeof(random)))
		ereport(ERROR, (errmsg("could not create changefeed generation")));
	for (int index = 0; index < 16; index++)
		snprintf(hex + index * 2, 3, "%02x", random[index]);
	generation = pstrdup(hex);
	{
		Oid insertTypes[] = {TEXTOID, TEXTOID, TEXTOID, OIDOID, TEXTOID, LSNOID, BOOLOID};
		Datum insertValues[] = {values[0], values[1], values[2],
							   ObjectIdGetDatum(relation),
							   CStringGetTextDatum(generation), LSNGetDatum(start),
							   BoolGetDatum(captureUpdates)};
		Execute("INSERT INTO documentdb_changefeed.feeds"
				" (database_name, collection_name, slot, relation_id, generation, start_lsn, storage_id,capture_updates)"
				" VALUES ($1,$2,$3,$4,$5,$6,pg_relation_filenode($4),$7)",
				7, insertTypes, insertValues, false);
	}
	if (captureUpdates)
	{
		char *table = quote_qualified_identifier(get_namespace_name(get_rel_namespace(relation)),
												get_rel_name(relation));
		char *name = psprintf("cf_update_%.16s", generation);
		Execute(psprintf("CREATE TRIGGER %s AFTER UPDATE ON %s"
						 " FOR EACH ROW EXECUTE FUNCTION documentdb_changefeed.capture_update(%s)",
						 quote_identifier(name), table, quote_literal_cstr(generation)),
				0, NULL, NULL, false);
	}
	SPI_finish();
	PG_RETURN_VOID();
}

Datum
documentdb_changefeed_unregister(PG_FUNCTION_ARGS)
{
	Oid types[] = {TEXTOID, TEXTOID};
	Datum values[] = {PG_GETARG_DATUM(0), PG_GETARG_DATUM(1)};
	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					   errmsg("changefeed registration requires superuser")));
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "could not connect to SPI");
	Execute("SELECT relation_id,generation,capture_updates FROM documentdb_changefeed.feeds"
			" WHERE database_name=$1 AND collection_name=$2", 2, types, values, true);
	if (SPI_processed == 1 && DatumGetBool(Column(3)))
	{
		Oid relation = DatumGetObjectId(Column(1));
		char *generation = TextDatumGetCString(Column(2));
		if (get_rel_name(relation) != NULL)
		{
			char *table = quote_qualified_identifier(get_namespace_name(get_rel_namespace(relation)),
													get_rel_name(relation));
			Execute(psprintf("DROP TRIGGER IF EXISTS %s ON %s",
							 quote_identifier(psprintf("cf_update_%.16s", generation)), table),
					0, NULL, NULL, false);
		}
	}
	Execute("DELETE FROM documentdb_changefeed.feeds"
			" WHERE database_name=$1 AND collection_name=$2",
			2, types, values, false);
	SPI_finish();
	PG_RETURN_VOID();
}

static char *
Token(const Feed *feed, XLogRecPtr position, int64 ordinal)
{
	return psprintf("cf1:%s:%X/%X:" INT64_FORMAT, feed->generation,
					LSN_FORMAT_ARGS(position), ordinal);
}

static void
ParseToken(const char *token, const Feed *feed, XLogRecPtr *position,
		   int64 *ordinal)
{
	char generation[33];
	unsigned high;
	unsigned low;
	int length = 0;
	if (strlen(token) > 128 ||
		sscanf(token, "cf1:%32[0-9a-f]:%X/%X:" INT64_FORMAT "%n",
			   generation, &high, &low, ordinal, &length) != 4 ||
		token[length] != '\0' || strcmp(generation, feed->generation) != 0 ||
		*ordinal < 0)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					   errmsg("invalid or out-of-scope changefeed token")));
	*position = ((uint64) high << 32) | low;
}

static char *
StringField(const bson_t *value, const char *field)
{
	bson_iter_t iterator;
	uint32 length;
	const char *text;
	if (!bson_iter_init_find(&iterator, value, field))
		return NULL;
	if (!BSON_ITER_HOLDS_UTF8(&iterator))
		ereport(ERROR, (errmsg("changefeed token must be a string")));
	text = bson_iter_utf8(&iterator, &length);
	if (length > 256 || memchr(text, '\0', length) != NULL)
		ereport(ERROR, (errmsg("invalid changefeed token string")));
	return pnstrdup(text, length);
}

static char *
ResumeToken(const bson_t *options)
{
	bson_iter_t iterator;
	char *token = NULL;
	if (!bson_iter_init(&iterator, options))
		elog(ERROR, "could not read changefeed options");
	while (bson_iter_next(&iterator))
	{
		if (strcmp(bson_iter_key(&iterator), "resumeAfter") == 0 &&
			BSON_ITER_HOLDS_DOCUMENT(&iterator))
		{
			const uint8_t *data;
			uint32 length;
			bson_t resume;
			char *encoded;
			int decoded;
			bson_iter_document(&iterator, &length, &data);
			bson_init_static(&resume, data, length);
			encoded = StringField(&resume, "_data");
			if (encoded == NULL)
				ereport(ERROR, (errmsg("resumeAfter requires _data")));
			token = palloc(257);
			decoded = pg_b64_decode(encoded, strlen(encoded), (Base64Byte *) token, 256);
			if (decoded < 0 || memchr(token, '\0', decoded) != NULL)
				ereport(ERROR, (errmsg("invalid encoded changefeed token")));
			token[decoded] = '\0';
		}
		else
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						   errmsg("unsupported reference changefeed option: %s",
								  bson_iter_key(&iterator))));
	}
	return token;
}

static bytea *
Continuation(const char *token)
{
	bson_t *result = bson_new();
	bytea *volatile bytes;
	PG_TRY();
	{
		BSON_APPEND_UTF8(result, "rt", token);
		bytes = CopyBson(result);
	}
	PG_FINALLY();
	{
		bson_destroy(result);
	}
	PG_END_TRY();
	return bytes;
}

static bytea *
EventDocument(const bson_t *event, const char *token,
			  const char *database, const char *collection)
{
	bson_t *delivered;
	bson_t *identity;
	bson_t *namespace;
	bson_iter_t field;
	char encoded[256];
	int length = pg_b64_encode((const Base64Byte *) token, strlen(token),
							  encoded, sizeof(encoded) - 1);
	bytea *volatile bytes;

	if (length < 0)
		elog(ERROR, "could not encode changefeed token");
	encoded[length] = '\0';
	delivered = bson_new();
	identity = bson_new();
	namespace = bson_new();
	PG_TRY();
	{
		BSON_APPEND_UTF8(identity, "_data", encoded);
		BSON_APPEND_DOCUMENT(delivered, "_id", identity);
		BSON_APPEND_UTF8(namespace, "db", database);
		BSON_APPEND_UTF8(namespace, "coll", collection);
		BSON_APPEND_DOCUMENT(delivered, "ns", namespace);
		bson_iter_init(&field, event);
		while (bson_iter_next(&field))
			if (strcmp(bson_iter_key(&field), "position") != 0 &&
				strcmp(bson_iter_key(&field), "ordinal") != 0)
				bson_append_value(delivered, bson_iter_key(&field), -1,
								  bson_iter_value(&field));
		bytes = CopyBson(delivered);
	}
	PG_FINALLY();
	{
		bson_destroy(namespace);
		bson_destroy(identity);
		bson_destroy(delivered);
	}
	PG_END_TRY();
	return bytes;
}

Datum
documentdb_changefeed_aggregate(PG_FUNCTION_ARGS)
{
	text *database = PG_GETARG_TEXT_PP(0);
	text *collection = PG_GETARG_TEXT_PP(1);
	bson_t options;
	bson_t cursor;
	Feed feed;
	char *resume;
	XLogRecPtr position;
	XLogRecPtr horizon;
	int64 ordinal = 0;
	int64 batchCount = 100;
	int64 batchBytes = 4 * 1024 * 1024;
	int64 count = 0;
	Size bytes = 0;
	bson_iter_t iterator;
	Oid types[] = {TEXTOID, TEXTOID};
	Datum args[] = {PointerGetDatum(database), PointerGetDatum(collection)};
	ReturnSetInfo *result = (ReturnSetInfo *) fcinfo->resultinfo;
	MemoryContext outputMemory;
	MemoryContext saved;
	Tuplestorestate *store;
	TupleDesc descriptor;
	bool hasMore = false;

	if (!ChangefeedEnabled || RecoveryInProgress())
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					   errmsg("reference changefeed provider is disabled")));
	ValidateNamespace(database);
	ValidateNamespace(collection);
	InitBson(PG_GETARG_DATUM(2), &options);
	InitBson(PG_GETARG_DATUM(3), &cursor);
	resume = ResumeToken(&options);
	{
		char *cursorToken = StringField(&cursor, "rt");
		if (cursorToken != NULL)
			resume = cursorToken;
	}
	if (bson_iter_init_find(&iterator, &cursor, "getpage_batchCount"))
	{
		if (!BSON_ITER_HOLDS_INT32(&iterator))
			ereport(ERROR, (errmsg("invalid changefeed batch count")));
		batchCount = Min(1000, bson_iter_int32(&iterator));
	}
	if (bson_iter_init_find(&iterator, &cursor, "getpage_batchSize"))
	{
		if (!BSON_ITER_HOLDS_INT32(&iterator))
			ereport(ERROR, (errmsg("invalid changefeed batch size")));
		batchBytes = Min(16 * 1024 * 1024, bson_iter_int32(&iterator));
	}
	if (batchCount < 0 || batchBytes <= 0)
		ereport(ERROR, (errmsg("invalid changefeed batch limits")));
	if (result == NULL || !IsA(result, ReturnSetInfo) ||
		!(result->allowedModes & SFRM_Materialize) ||
		get_call_result_type(fcinfo, NULL, &descriptor) != TYPEFUNC_COMPOSITE)
		ereport(ERROR, (errmsg("changefeed requires a composite result context")));
	outputMemory = result->econtext->ecxt_per_query_memory;
	saved = MemoryContextSwitchTo(outputMemory);
	descriptor = BlessTupleDesc(CreateTupleDescCopy(descriptor));
	store = tuplestore_begin_heap(true, false, 4096);
	result->returnMode = SFRM_Materialize;
	result->setResult = store;
	result->setDesc = descriptor;
	MemoryContextSwitchTo(saved);
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "could not connect to SPI");
	CheckRetention();
	Execute("SELECT f.relation_id, f.slot, f.generation, f.start_lsn"
			" FROM documentdb_changefeed.feeds f"
			" JOIN documentdb_api_catalog.collections c"
			" ON c.database_name=f.database_name AND c.collection_name=f.collection_name"
			" WHERE f.database_name=$1 AND f.collection_name=$2"
			" AND f.relation_id=('documentdb_data.documents_' || c.collection_id)::regclass"
			" AND f.storage_id=pg_relation_filenode(f.relation_id)",
			2, types, args, true);
	if (SPI_processed != 1)
		ereport(ERROR, (errmsg("changefeed is not registered or collection history changed")));
	feed.relation = DatumGetObjectId(Column(1));
	feed.slot = TextDatumGetCString(Column(2));
	feed.generation = TextDatumGetCString(Column(3));
	feed.start = DatumGetLSN(Column(4));
	if (pg_class_aclcheck(feed.relation, GetOuterUserId(), ACL_SELECT) != ACLCHECK_OK)
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					   errmsg("changefeed requires collection read permission")));
	{
		Oid lockType = INT8OID;
		Datum lockKey = Int64GetDatum(((uint64) 0x43463101 << 32) | feed.relation);
		/* Serialize slot peeks without consuming another reader's history. */
		Execute("SELECT pg_advisory_xact_lock($1)", 1, &lockType, &lockKey, true);
	}
	{
		Oid slotType = TEXTOID;
		Datum slotValue = CStringGetTextDatum(feed.slot);
		Execute("SELECT restart_lsn, confirmed_flush_lsn, pg_current_wal_flush_lsn()"
				" FROM pg_replication_slots WHERE slot_name=$1"
				" AND database=current_database() AND plugin='pg_documentdb_changefeed'"
				" AND NOT two_phase AND wal_status IN ('reserved','extended')",
				1, &slotType, &slotValue, true);
		if (SPI_processed != 1)
			ereport(ERROR, (errmsg("changefeed slot or retained history is unavailable")));
		horizon = DatumGetLSN(Column(3));
		if (DatumGetLSN(Column(2)) != feed.start)
			ereport(ERROR, (errmsg("changefeed slot has been advanced or replaced")));
		if (horizon - DatumGetLSN(Column(1)) > 256 * 1024 * 1024)
			ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						   errmsg("changefeed retained scan exceeds 256 MiB; close the feed")));
		position = horizon;
		if (resume != NULL)
		{
			ParseToken(resume, &feed, &position, &ordinal);
			if (position < feed.start || position < DatumGetLSN(Column(2)) ||
				position > horizon)
				ereport(ERROR, (errmsg("changefeed checkpoint history is unavailable")));
		}
	}
	if (resume != NULL && batchCount > 0)
	{
		Oid readTypes[] = {TEXTOID, LSNOID, TEXTOID, TEXTOID, TEXTOID};
		Datum readArgs[] = {CStringGetTextDatum(feed.slot), LSNGetDatum(horizon),
						   CStringGetTextDatum(psprintf("%u", feed.relation)),
						   CStringGetTextDatum(psprintf("%X/%X", LSN_FORMAT_ARGS(position))),
						   CStringGetTextDatum(feed.generation)};
		XLogRecPtr previousPosition = position;
		int64 previousOrdinal = ordinal;
		Execute("SELECT data FROM pg_logical_slot_peek_binary_changes"
				" ($1,$2,NULL,'relation',$3,'after',$4,'generation',$5)",
				5, readTypes, readArgs, true);
		for (uint64 index = 0; index < SPI_processed; index++)
		{
			bool isNull;
			Datum data = SPI_getbinval(SPI_tuptable->vals[index], SPI_tuptable->tupdesc,
									  1, &isNull);
			bson_t event;
			bytea *delivered;
			bson_iter_t field;
			char *eventPosition;
			XLogRecPtr current;
			int64 currentOrdinal;
			char *rawToken;
			Datum values[2];
			bool nulls[2] = {false, false};

			CHECK_FOR_INTERRUPTS();
			if (isNull)
				ereport(ERROR, (errmsg("changefeed output is missing")));
			InitBson(data, &event);
			eventPosition = StringField(&event, "position");
			if (eventPosition == NULL ||
				!bson_iter_init_find(&field, &event, "ordinal") ||
				!BSON_ITER_HOLDS_INT64(&field))
				ereport(ERROR, (errmsg("invalid changefeed output position")));
			current = DatumGetLSN(DirectFunctionCall1(pg_lsn_in,
											 CStringGetDatum(eventPosition)));
			currentOrdinal = bson_iter_int64(&field);
			if (current < previousPosition ||
				(current == previousPosition && currentOrdinal <= previousOrdinal))
				continue;
			if (count >= batchCount)
			{
				hasMore = true;
				break;
			}
			rawToken = Token(&feed, current, currentOrdinal);
			delivered = EventDocument(&event, rawToken, text_to_cstring(database),
									  text_to_cstring(collection));
			if (VARSIZE_ANY_EXHDR(delivered) > batchBytes - bytes)
			{
				if (count == 0)
					ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
								   errmsg("changefeed event exceeds the page byte limit")));
				hasMore = true;
				break;
			}
			position = current;
			ordinal = currentOrdinal;
			values[0] = PointerGetDatum(delivered);
			values[1] = PointerGetDatum(Continuation(rawToken));
			tuplestore_putvalues(store, descriptor, values, nulls);
			bytes += VARSIZE_ANY_EXHDR(delivered);
			count++;
		}
		if (!hasMore)
		{
			position = horizon;
			ordinal = 0;
		}
	}
	{
		Datum values[] = {0, PointerGetDatum(Continuation(Token(&feed, position, ordinal)))};
		bool nulls[] = {true, false};
		tuplestore_putvalues(store, descriptor, values, nulls);
	}
	SPI_finish();
	return (Datum) 0;
}
