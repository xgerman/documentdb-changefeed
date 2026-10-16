#include "postgres.h"

#include <fcntl.h>
#include <unistd.h>

#include "access/detoast.h"
#include "access/heapam_xlog.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "catalog/pg_type_d.h"
#include "commands/extension.h"
#include "common/hashfn.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include "changefeed.h"

typedef struct ChunkKey
{
	Oid valueId;
	int32 sequence;
} ChunkKey;

typedef struct Chunk
{
	ChunkKey key;
	bytea *data;
} Chunk;

typedef struct Change
{
	struct Change *next;
	XLogRecPtr lsn;
	int ordinal;
	char operation;
	HeapTuple before;
	HeapTuple after;
} Change;

typedef struct Transaction
{
	TransactionId xid;
	MemoryContext memory;
	Change *first;
	Change *last;
	HTAB *chunks;
} Transaction;

typedef struct RowKey
{
	Size length;
	const char *data;
} RowKey;

typedef struct RowImage
{
	RowKey key;
	Oid toastValueId;
	bytea *image;
} RowImage;

typedef struct Decoder
{
	MemoryContext memory;
	MemoryContext eventMemory;
	XLogReaderState *reader;
	XLogRecPtr end;
	TimeLineID timeline;
	Relation relation;
	Relation toast;
	AttrNumber documentAttribute;
	AttrNumber identityAttribute;
	bool bsonIdentity;
	RelFileLocator locator;
	RelFileLocator toastLocator;
	HTAB *transactions;
	HTAB *rows;
	Tuplestorestate *output;
	TupleDesc outputDescriptor;
	Size budget;
	Size outputBytes;
} Decoder;

PG_FUNCTION_INFO_V1(documentdb_changefeed_decode_test);

static void
CheckBudget(Decoder *decoder, Size allocation)
{
	Size used = MemoryContextMemAllocated(decoder->memory, true);

	CHECK_FOR_INTERRUPTS();
	if (allocation > decoder->budget ||
		used > decoder->budget - allocation ||
		decoder->outputBytes > decoder->budget - allocation - used)
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("changefeed fixture decoder memory budget exceeded")));
	}
}

static HTAB *
CreateHash(const char *name, Size keySize, Size entrySize, MemoryContext memory)
{
	HASHCTL control = {0};

	control.keysize = keySize;
	control.entrysize = entrySize;
	control.hcxt = memory;
	return hash_create(name, 32, &control, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static uint32
HashRowKey(const void *key, Size keySize)
{
	const RowKey *row = key;

	return hash_bytes((const unsigned char *) row->data, row->length);
}

static int
CompareRowKeys(const void *left, const void *right, Size keySize)
{
	const RowKey *a = left;
	const RowKey *b = right;

	if (a->length != b->length)
		return a->length < b->length ? -1 : 1;
	return memcmp(a->data, b->data, a->length);
}

static HTAB *
CreateRowHash(MemoryContext memory)
{
	HASHCTL control = {0};

	control.keysize = sizeof(RowKey);
	control.entrysize = sizeof(RowImage);
	control.hcxt = memory;
	control.hash = HashRowKey;
	control.match = CompareRowKeys;
	return hash_create("changefeed fixture prior images", 32, &control,
					   HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);
}

static Oid
DocumentdbBsonType(void)
{
	Oid namespace = get_namespace_oid("documentdb_core", true);
	Oid type;
	Oid extension = get_extension_oid("documentdb_core", true);

	if (!OidIsValid(namespace) || !OidIsValid(extension))
		return InvalidOid;
	type = GetSysCacheOid2(TYPENAMENSP, Anum_pg_type_oid,
						  CStringGetDatum("bson"), ObjectIdGetDatum(namespace));
	if (!OidIsValid(type) || getExtensionOfObject(TypeRelationId, type) != extension)
		return InvalidOid;
	return type;
}

static void
CloseSegment(XLogReaderState *reader)
{
	if (reader->seg.ws_file >= 0)
	{
		CloseTransientFile(reader->seg.ws_file);
		reader->seg.ws_file = -1;
	}
}

static int
ReadPage(XLogReaderState *reader, XLogRecPtr page, int required,
		 XLogRecPtr record, char *buffer)
{
	Decoder *decoder = reader->private_data;
	XLogSegNo segment;
	char name[MAXFNAMELEN];
	char path[MAXPGPATH];
	int available;
	int received;

	CHECK_FOR_INTERRUPTS();
	if (page >= decoder->end || (uint64) required > decoder->end - page)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("changefeed fixture WAL boundary cuts an incomplete record")));
	}
	available = Min((uint64) XLOG_BLCKSZ, decoder->end - page);
	XLByteToSeg(page, segment, wal_segment_size);
	if (reader->seg.ws_file < 0 || reader->seg.ws_segno != segment)
	{
		CloseSegment(reader);
		XLogFileName(name, decoder->timeline, segment, wal_segment_size);
		snprintf(path, sizeof(path), XLOGDIR "/%s", name);
		reader->seg.ws_file = OpenTransientFile(path, O_RDONLY | PG_BINARY);
		if (reader->seg.ws_file < 0)
		{
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open retained changefeed WAL file \"%s\": %m", path)));
		}
		reader->seg.ws_segno = segment;
		reader->seg.ws_tli = decoder->timeline;
	}
	received = pg_pread(reader->seg.ws_file, buffer, available,
						XLogSegmentOffset(page, wal_segment_size));
	if (received != available)
	{
		ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
				 errmsg("could not read complete retained changefeed WAL page")));
	}
	return received;
}

static Transaction *
GetTransaction(Decoder *decoder, TransactionId xid)
{
	bool found;
	Transaction *transaction;

	CheckBudget(decoder, sizeof(Transaction));
	transaction = hash_search(decoder->transactions, &xid, HASH_ENTER, &found);
	if (!found)
	{
		transaction->memory = AllocSetContextCreate(decoder->memory,
												   "changefeed fixture transaction",
												   ALLOCSET_SMALL_SIZES);
		transaction->first = NULL;
		transaction->last = NULL;
		transaction->chunks = CreateHash("changefeed fixture chunks",
										 sizeof(ChunkKey), sizeof(Chunk),
										 transaction->memory);
	}
	return transaction;
}

static void
ForgetTransaction(Decoder *decoder, TransactionId xid)
{
	Transaction *transaction = hash_search(decoder->transactions, &xid, HASH_FIND, NULL);

	if (transaction != NULL)
	{
		MemoryContextDelete(transaction->memory);
		hash_search(decoder->transactions, &xid, HASH_REMOVE, NULL);
	}
}

static HeapTuple
ReadTuple(Decoder *decoder, const xl_heap_header *header, const char *body, Size length)
{
	HeapTuple tuple;
	Size tupleLength;

	if (length > MaxAllocSize - SizeofHeapTupleHeader)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid WAL tuple size")));
	tupleLength = length + SizeofHeapTupleHeader;
	if (header->t_hoff < SizeofHeapTupleHeader || header->t_hoff > tupleLength)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid WAL tuple header")));
	CheckBudget(decoder, HEAPTUPLESIZE + tupleLength);
	tuple = palloc0(HEAPTUPLESIZE + tupleLength);
	tuple->t_data = (HeapTupleHeader) ((char *) tuple + HEAPTUPLESIZE);
	tuple->t_len = tupleLength;
	tuple->t_data->t_infomask2 = header->t_infomask2;
	tuple->t_data->t_infomask = header->t_infomask;
	tuple->t_data->t_hoff = header->t_hoff;
	memcpy((char *) tuple->t_data + SizeofHeapTupleHeader, body, length);
	return tuple;
}

static HeapTuple
ReadLoggedTuple(Decoder *decoder, const char *data, Size length)
{
	xl_heap_header header;

	if (length < SizeOfHeapHeader)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("missing WAL tuple header")));
	memcpy(&header, data, SizeOfHeapHeader);
	return ReadTuple(decoder, &header, data + SizeOfHeapHeader, length - SizeOfHeapHeader);
}

static Datum
Attribute(HeapTuple tuple, TupleDesc descriptor, int attribute)
{
	bool isNull;
	Datum result;

	if (HeapTupleHeaderGetNatts(tuple->t_data) != descriptor->natts)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("fixture decoder requires an unchanged table descriptor")));
	}
	result = heap_getattr(tuple, attribute, descriptor, &isNull);
	if (isNull)
		ereport(ERROR, (errcode(ERRCODE_NOT_NULL_VIOLATION), errmsg("null fixture attribute")));
	return result;
}

static RowKey
Identity(Decoder *decoder, HeapTuple tuple)
{
	Datum datum = Attribute(tuple, RelationGetDescr(decoder->relation),
							decoder->identityAttribute);
	RowKey key;

	if (decoder->bsonIdentity)
	{
		struct varlena *value = (struct varlena *) DatumGetPointer(datum);

		if (VARATT_IS_EXTERNAL(value))
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("fixture BSON row identifiers must be inline")));
		CheckBudget(decoder, toast_raw_datum_size(datum));
		value = detoast_attr(value);
		key.length = VARSIZE_ANY_EXHDR(value);
		key.data = VARDATA_ANY(value);
	}
	else
	{
		int64 id = DatumGetInt64(datum);
		char *bytes;

		CheckBudget(decoder, sizeof(id));
		bytes = palloc(sizeof(id));
		memcpy(bytes, &id, sizeof(id));
		key.length = sizeof(id);
		key.data = bytes;
	}
	return key;
}

static void
AddTuple(Decoder *decoder, Transaction *transaction, bool isToast,
		 char operation, int ordinal, HeapTuple before, HeapTuple after)
{
	if (isToast)
	{
		ChunkKey key = {0};
		Chunk *chunk;
		bool found;
		bytea *data;
		TupleDesc descriptor = RelationGetDescr(decoder->toast);

		/* Deleted TOAST chunks are not a source of historical image bytes. */
		if (operation != 'i')
			return;
		key.valueId = DatumGetObjectId(Attribute(after, descriptor, 1));
		key.sequence = DatumGetInt32(Attribute(after, descriptor, 2));
		data = DatumGetByteaPP(Attribute(after, descriptor, 3));
		CheckBudget(decoder, VARSIZE_ANY(data) + sizeof(Chunk));
		chunk = hash_search(transaction->chunks, &key, HASH_ENTER, &found);
		if (found || key.sequence < 0)
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("duplicate or invalid TOAST chunk")));
		chunk->data = DatumGetByteaPCopy(PointerGetDatum(data));
		return;
	}
	else
	{
		Change *change;

		CheckBudget(decoder, sizeof(Change));
		change = palloc0(sizeof(Change));
		change->lsn = decoder->reader->ReadRecPtr;
		change->ordinal = ordinal;
		change->operation = operation;
		change->before = before;
		change->after = after;
		if (transaction->last != NULL)
			transaction->last->next = change;
		else
			transaction->first = change;
		transaction->last = change;
	}
}

static void
DecodeHeap(Decoder *decoder)
{
	XLogReaderState *reader = decoder->reader;
	RelFileLocator locator;
	ForkNumber fork;
	BlockNumber block;
	TransactionId xid = XLogRecGetXid(reader);
	uint8 opcode = XLogRecGetInfo(reader) & XLOG_HEAP_OPMASK;
	char *data;
	Size length;
	bool isToast;
	Transaction *transaction;
	MemoryContext old;

	if (XLogRecGetRmid(reader) == RM_HEAP_ID && opcode == XLOG_HEAP_TRUNCATE)
	{
		xl_heap_truncate *record = (xl_heap_truncate *) XLogRecGetData(reader);
		uint32 i;

		if (record->dbId == MyDatabaseId)
			for (i = 0; i < record->nrelids; i++)
				if (record->relids[i] == RelationGetRelid(decoder->relation))
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("fixture decoder does not support table truncation")));
		return;
	}
	if (!TransactionIdIsValid(xid) || !XLogRecHasBlockRef(reader, 0))
		return;
	XLogRecGetBlockTag(reader, 0, &locator, &fork, &block);
	isToast = decoder->toast != NULL &&
		memcmp(&locator, &decoder->toastLocator, sizeof(locator)) == 0;
	if (fork != MAIN_FORKNUM ||
		(!isToast && memcmp(&locator, &decoder->locator, sizeof(locator)) != 0))
		return;
	transaction = GetTransaction(decoder, xid);
	old = MemoryContextSwitchTo(transaction->memory);
	data = XLogRecGetBlockData(reader, 0, &length);
	if (XLogRecGetRmid(reader) == RM_HEAP2_ID)
	{
		if (opcode == XLOG_HEAP2_MULTI_INSERT)
		{
			xl_heap_multi_insert *record = (xl_heap_multi_insert *) XLogRecGetData(reader);
			const char *cursor = data;
			const char *end = data != NULL ? data + length : NULL;
			int i;

			if (XLogRecGetDataLen(reader) < SizeOfHeapMultiInsert ||
				!(record->flags & XLH_INSERT_CONTAINS_NEW_TUPLE) || data == NULL)
				ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("missing multi-insert tuples")));
			for (i = 0; i < record->ntuples; i++)
			{
				xl_multi_insert_tuple multi;
				xl_heap_header header;

				cursor = (const char *) TYPEALIGN(2, (uintptr_t) cursor);
				if (cursor > end || (Size) (end - cursor) < SizeOfMultiInsertTuple)
					ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid multi-insert header")));
				memcpy(&multi, cursor, SizeOfMultiInsertTuple);
				cursor += SizeOfMultiInsertTuple;
				if (end - cursor < multi.datalen)
					ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid multi-insert data")));
				header.t_infomask = multi.t_infomask;
				header.t_infomask2 = multi.t_infomask2;
				header.t_hoff = multi.t_hoff;
				AddTuple(decoder, transaction, isToast, 'i', i, NULL,
						 ReadTuple(decoder, &header, cursor, multi.datalen));
				cursor += multi.datalen;
			}
		}
	}
	else if (opcode == XLOG_HEAP_INSERT)
	{
		xl_heap_insert *record = (xl_heap_insert *) XLogRecGetData(reader);

		if (XLogRecGetDataLen(reader) < SizeOfHeapInsert ||
			!(record->flags & XLH_INSERT_CONTAINS_NEW_TUPLE) || data == NULL)
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("missing inserted WAL tuple")));
		if (record->flags & XLH_INSERT_IS_SPECULATIVE)
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("speculative inserts are not supported by the fixture decoder")));
		AddTuple(decoder, transaction, isToast, 'i', 0, NULL,
				 ReadLoggedTuple(decoder, data, length));
	}
	else if (!isToast && (opcode == XLOG_HEAP_UPDATE || opcode == XLOG_HEAP_HOT_UPDATE))
	{
		xl_heap_update *record = (xl_heap_update *) XLogRecGetData(reader);
		HeapTuple before;
		HeapTuple after;

		if (XLogRecGetDataLen(reader) < SizeOfHeapUpdate ||
			!(record->flags & XLH_UPDATE_CONTAINS_OLD_TUPLE) ||
			!(record->flags & XLH_UPDATE_CONTAINS_NEW_TUPLE) || data == NULL ||
			(record->flags & (XLH_UPDATE_PREFIX_FROM_OLD | XLH_UPDATE_SUFFIX_FROM_OLD)))
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("fixture updates require FULL replica identity and complete logical WAL tuples")));
		before = ReadLoggedTuple(decoder, XLogRecGetData(reader) + SizeOfHeapUpdate,
								 XLogRecGetDataLen(reader) - SizeOfHeapUpdate);
		after = ReadLoggedTuple(decoder, data, length);
		AddTuple(decoder, transaction, false, 'u', 0, before, after);
	}
	else if (!isToast && opcode == XLOG_HEAP_DELETE)
	{
		xl_heap_delete *record = (xl_heap_delete *) XLogRecGetData(reader);

		if (XLogRecGetDataLen(reader) < SizeOfHeapDelete ||
			!(record->flags & XLH_DELETE_CONTAINS_OLD_TUPLE))
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("fixture deletes require FULL replica identity")));
		AddTuple(decoder, transaction, false, 'd', 0,
				 ReadLoggedTuple(decoder, XLogRecGetData(reader) + SizeOfHeapDelete,
								 XLogRecGetDataLen(reader) - SizeOfHeapDelete), NULL);
	}
	MemoryContextSwitchTo(old);
}

static bool
BelongsTo(TransactionId xid, TransactionId top, int count, TransactionId *children)
{
	int i;

	if (xid == top)
		return true;
	for (i = 0; i < count; i++)
		if (xid == children[i])
			return true;
	return false;
}

static Chunk *
FindChunk(Decoder *decoder, ChunkKey *key, TransactionId top,
		  int childCount, TransactionId *children)
{
	HASH_SEQ_STATUS scan;
	Transaction *transaction;
	Chunk *result = NULL;

	hash_seq_init(&scan, decoder->transactions);
	while ((transaction = hash_seq_search(&scan)) != NULL)
	{
		if (BelongsTo(transaction->xid, top, childCount, children))
		{
			Chunk *chunk = hash_search(transaction->chunks, key, HASH_FIND, NULL);

			if (chunk != NULL)
			{
				if (result != NULL)
					ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("ambiguous historical TOAST chunk")));
				result = chunk;
			}
		}
	}
	return result;
}

static Oid
ToastIdentity(Datum datum)
{
	struct varlena *value = (struct varlena *) DatumGetPointer(datum);
	varatt_external external;

	if (!VARATT_IS_EXTERNAL_ONDISK(value))
		return InvalidOid;
	VARATT_EXTERNAL_GET_POINTER(external, value);
	return external.va_valueid;
}

static bytea *
Image(Decoder *decoder, Datum datum, RowImage *prior,
	  TransactionId top, int childCount, TransactionId *children)
{
	struct varlena *value = (struct varlena *) DatumGetPointer(datum);

	if (VARATT_IS_EXTERNAL_ONDISK(value))
	{
		varatt_external external;
		ChunkKey key = {0};
		Size size;
		Size copied = 0;
		struct varlena *assembled;

		VARATT_EXTERNAL_GET_POINTER(external, value);
		if (decoder->toast == NULL ||
			external.va_toastrelid != RelationGetRelid(decoder->toast))
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("unexpected historical TOAST relation")));
		key.valueId = external.va_valueid;
		/* Reuse only an image whose physical TOAST identity was seen in WAL. */
		if (FindChunk(decoder, &key, top, childCount, children) == NULL &&
			prior != NULL && prior->toastValueId == key.valueId && prior->image != NULL)
			return DatumGetByteaPCopy(PointerGetDatum(prior->image));
		size = VARATT_EXTERNAL_GET_EXTSIZE(external);
		if (external.va_rawsize < VARHDRSZ)
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid historical TOAST raw size")));
		CheckBudget(decoder, size + external.va_rawsize + VARHDRSZ);
		assembled = palloc(size + VARHDRSZ);
		if (size < (Size) external.va_rawsize - VARHDRSZ)
			SET_VARSIZE_COMPRESSED(assembled, size + VARHDRSZ);
		else
			SET_VARSIZE(assembled, size + VARHDRSZ);
		while (copied < size)
		{
			Chunk *chunk = FindChunk(decoder, &key, top, childCount, children);
			Size bytes;

			CHECK_FOR_INTERRUPTS();
			if (chunk == NULL)
				ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("required historical TOAST chunk is not in the fixture WAL range")));
			bytes = VARSIZE_ANY_EXHDR(chunk->data);
			if (bytes == 0 || bytes > size - copied)
				ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid historical TOAST chunk size")));
			memcpy(VARDATA(assembled) + copied, VARDATA_ANY(chunk->data), bytes);
			copied += bytes;
			key.sequence++;
		}
		value = detoast_attr(assembled);
		if (VARSIZE_ANY(value) != (Size) external.va_rawsize)
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("historical TOAST image size mismatch")));
	}
	else
	{
		if (VARATT_IS_EXTERNAL(value))
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported historical external datum")));
		CheckBudget(decoder, toast_raw_datum_size(datum));
		value = detoast_attr(value);
	}
	return DatumGetByteaPCopy(PointerGetDatum(value));
}

static int
CompareChanges(const void *left, const void *right)
{
	const Change *a = *(Change *const *) left;
	const Change *b = *(Change *const *) right;

	if (a->lsn != b->lsn)
		return a->lsn < b->lsn ? -1 : 1;
	return (a->ordinal > b->ordinal) - (a->ordinal < b->ordinal);
}

static void
EmitChange(Decoder *decoder, Change *change, TransactionId top,
		   int childCount, TransactionId *children)
{
	Datum values[7] = {0};
	bool nulls[7] = {false};
	HeapTuple source = change->after != NULL ? change->after : change->before;
	TupleDesc descriptor = RelationGetDescr(decoder->relation);
	RowKey key = Identity(decoder, source);
	RowImage *prior = hash_search(decoder->rows, &key, HASH_FIND, NULL);
	bytea *before = NULL;
	bytea *after = NULL;
	Datum newDatum = (Datum) 0;

	if (change->operation == 'i' && prior != NULL)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("fixture row identifiers must be unique")));
	if (change->before != NULL && change->after != NULL)
	{
		RowKey beforeKey = Identity(decoder, change->before);

		if (CompareRowKeys(&beforeKey, &key, sizeof(RowKey)) != 0)
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("fixture row identifiers must not change")));
	}
	if (change->before != NULL)
		before = Image(decoder, Attribute(change->before, descriptor, decoder->documentAttribute),
					   prior, top, childCount, children);
	if (change->after != NULL)
	{
		newDatum = Attribute(change->after, descriptor, decoder->documentAttribute);
		after = Image(decoder, newDatum, prior, top, childCount, children);
	}
	values[0] = LSNGetDatum(decoder->reader->ReadRecPtr);
	values[1] = LSNGetDatum(change->lsn);
	values[2] = Int32GetDatum(change->ordinal);
	values[3] = Int64GetDatum(top);
	values[4] = CStringGetTextDatum(change->operation == 'i' ? "insert" :
								  change->operation == 'u' ? "update" : "delete");
	nulls[5] = before == NULL;
	nulls[6] = after == NULL;
	values[5] = PointerGetDatum(before);
	values[6] = PointerGetDatum(after);
	CheckBudget(decoder, (before != NULL ? VARSIZE(before) : 0) +
				(after != NULL ? VARSIZE(after) : 0) + 128);
	decoder->outputBytes += (before != NULL ? VARSIZE(before) : 0) +
		(after != NULL ? VARSIZE(after) : 0) + 128;
	tuplestore_putvalues(decoder->output, decoder->outputDescriptor, values, nulls);
	if (after != NULL)
	{
		MemoryContext old;
		RowImage *row;
		bool found;

		CheckBudget(decoder, VARSIZE(after) + sizeof(RowImage) + key.length);
		row = hash_search(decoder->rows, &key, HASH_ENTER, &found);
		if (found && row->image != NULL)
			pfree(row->image);
		old = MemoryContextSwitchTo(decoder->memory);
		if (!found)
			row->key.data = palloc(key.length);
		if (!found)
			memcpy((char *) row->key.data, key.data, key.length);
		row->image = DatumGetByteaPCopy(PointerGetDatum(after));
		row->toastValueId = ToastIdentity(newDatum);
		MemoryContextSwitchTo(old);
	}
	else if (prior != NULL)
	{
		const char *storedKey = prior->key.data;

		pfree(prior->image);
		hash_search(decoder->rows, &key, HASH_REMOVE, NULL);
		pfree((void *) storedKey);
	}
}

static void
Commit(Decoder *decoder, TransactionId top, int childCount, TransactionId *children)
{
	HASH_SEQ_STATUS scan;
	Transaction *transaction;
	Change **changes;
	int count = 0;
	int index = 0;
	int i;

	hash_seq_init(&scan, decoder->transactions);
	while ((transaction = hash_seq_search(&scan)) != NULL)
		if (BelongsTo(transaction->xid, top, childCount, children))
			for (Change *change = transaction->first; change != NULL; change = change->next)
				count++;
	CheckBudget(decoder, Max(count, 1) * sizeof(Change *));
	changes = palloc(Max(count, 1) * sizeof(Change *));
	hash_seq_init(&scan, decoder->transactions);
	while ((transaction = hash_seq_search(&scan)) != NULL)
		if (BelongsTo(transaction->xid, top, childCount, children))
			for (Change *change = transaction->first; change != NULL; change = change->next)
				changes[index++] = change;
	qsort(changes, count, sizeof(Change *), CompareChanges);
	for (i = 0; i < count; i++)
	{
		MemoryContext old = MemoryContextSwitchTo(decoder->eventMemory);

		EmitChange(decoder, changes[i], top, childCount, children);
		MemoryContextSwitchTo(old);
		MemoryContextReset(decoder->eventMemory);
	}
	pfree(changes);
	ForgetTransaction(decoder, top);
	for (i = 0; i < childCount; i++)
		ForgetTransaction(decoder, children[i]);
}

static void
ScanWal(Decoder *decoder, XLogRecPtr start)
{
	XLogReaderState *reader = decoder->reader;
	XLogRecPtr first = XLogFindNextRecord(reader, start);

	if (XLogRecPtrIsInvalid(first) || first >= decoder->end)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("fixture start is not a readable WAL boundary")));
	XLogBeginRead(reader, first);
	while (reader->EndRecPtr < decoder->end)
	{
		char *error = NULL;
		TransactionId xid;
		uint8 manager;
		uint8 opcode;

		CheckBudget(decoder, 0);
		if (XLogReadRecord(reader, &error) == NULL)
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("could not decode fixture WAL: %s", error != NULL ? error : "unexpected end")));
		manager = XLogRecGetRmid(reader);
		xid = XLogRecGetXid(reader);
		opcode = XLogRecGetInfo(reader) & XLOG_XACT_OPMASK;
		if (manager == RM_HEAP_ID || manager == RM_HEAP2_ID)
			DecodeHeap(decoder);
		else if (manager == RM_XACT_ID &&
				 (opcode == XLOG_XACT_COMMIT || opcode == XLOG_XACT_COMMIT_PREPARED))
		{
			xl_xact_parsed_commit parsed;

			ParseCommitRecord(XLogRecGetInfo(reader), (xl_xact_commit *) XLogRecGetData(reader), &parsed);
			if (TransactionIdIsValid(parsed.twophase_xid))
				xid = parsed.twophase_xid;
			Commit(decoder, xid, parsed.nsubxacts, parsed.subxacts);
		}
		else if (manager == RM_XACT_ID &&
				 (opcode == XLOG_XACT_ABORT || opcode == XLOG_XACT_ABORT_PREPARED))
		{
			xl_xact_parsed_abort parsed;
			int i;

			ParseAbortRecord(XLogRecGetInfo(reader), (xl_xact_abort *) XLogRecGetData(reader), &parsed);
			if (TransactionIdIsValid(parsed.twophase_xid))
				xid = parsed.twophase_xid;
			ForgetTransaction(decoder, xid);
			for (i = 0; i < parsed.nsubxacts; i++)
				ForgetTransaction(decoder, parsed.subxacts[i]);
		}
	}
	if (reader->EndRecPtr != decoder->end)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("fixture end must be an aligned WAL record boundary")));
}

Datum
documentdb_changefeed_decode_test(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);
	XLogRecPtr start = PG_GETARG_LSN(1);
	XLogRecPtr end = PG_GETARG_LSN(2);
	int memoryMb = PG_GETARG_INT32(3);
	MemoryContext caller = CurrentMemoryContext;
	Decoder *decoder;
	XLogReaderRoutine routine = {0};
	ReturnSetInfo *result = (ReturnSetInfo *) fcinfo->resultinfo;
	XLogRecPtr flush;
	AttrNumber documentAttribute;
	Oid documentType;
	Oid bsonType;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("fixture WAL decoding requires superuser")));
	if (RecoveryInProgress() || wal_level < WAL_LEVEL_LOGICAL)
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("fixture decoder requires a primary with wal_level=logical")));
	if (memoryMb < 1 || memoryMb > 256)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("fixture memory limit must be between 1 and 256 MiB")));
	if (XLogRecPtrIsInvalid(start) || start >= end || end - start > 256 * 1024 * 1024)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("fixture WAL range must be positive and at most 256 MiB")));

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);
	decoder = palloc0(sizeof(Decoder));
	decoder->memory = AllocSetContextCreate(caller, "changefeed fixture decoder", ALLOCSET_DEFAULT_SIZES);
	decoder->eventMemory = AllocSetContextCreate(decoder->memory, "changefeed fixture event", ALLOCSET_SMALL_SIZES);
	decoder->budget = (Size) memoryMb * 1024 * 1024;
	decoder->output = result->setResult;
	decoder->outputDescriptor = result->setDesc;
	flush = GetFlushRecPtr(&decoder->timeline);
	if (end > flush)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("fixture end is beyond the durable WAL horizon")));
	decoder->end = end;
	PG_TRY();
	{
		decoder->relation = table_open(relationId, AccessShareLock);
		decoder->locator = decoder->relation->rd_locator;
		documentAttribute = get_attnum(relationId, "document");
		documentType = documentAttribute > 0 ?
			TupleDescAttr(RelationGetDescr(decoder->relation), documentAttribute - 1)->atttypid : InvalidOid;
		bsonType = DocumentdbBsonType();
		decoder->bsonIdentity = OidIsValid(bsonType) && documentType == bsonType;
		decoder->identityAttribute = decoder->bsonIdentity ? get_attnum(relationId, "object_id") : 1;
		if (RelationGetDescr(decoder->relation)->natts < 2 || documentAttribute <= 1 ||
			decoder->identityAttribute < 1 ||
			(decoder->bsonIdentity ?
			 TupleDescAttr(RelationGetDescr(decoder->relation), decoder->identityAttribute - 1)->atttypid != bsonType :
			 (documentType != BYTEAOID || TupleDescAttr(RelationGetDescr(decoder->relation), 0)->atttypid != INT8OID)) ||
			decoder->relation->rd_rel->relreplident != REPLICA_IDENTITY_FULL ||
			decoder->relation->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT ||
			decoder->relation->rd_rel->relkind != RELKIND_RELATION)
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("fixture requires a permanent FULL-identity DocumentDB BSON collection or bigint/bytea fixture table")));
		decoder->documentAttribute = documentAttribute;
		if (OidIsValid(decoder->relation->rd_rel->reltoastrelid))
		{
			decoder->toast = table_open(decoder->relation->rd_rel->reltoastrelid, AccessShareLock);
			decoder->toastLocator = decoder->toast->rd_locator;
		}
		MemoryContextSwitchTo(decoder->memory);
		decoder->transactions = CreateHash("changefeed fixture transactions", sizeof(TransactionId), sizeof(Transaction), decoder->memory);
		decoder->rows = CreateRowHash(decoder->memory);
		routine.page_read = ReadPage;
		routine.segment_close = CloseSegment;
		decoder->reader = XLogReaderAllocate(wal_segment_size, NULL, &routine, decoder);
		if (decoder->reader == NULL)
			ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("could not allocate fixture WAL reader")));
		ScanWal(decoder, start);
		CheckBudget(decoder, 0);
		MemoryContextSwitchTo(caller);
	}
	PG_FINALLY();
	{
		MemoryContextSwitchTo(caller);
		if (decoder->reader != NULL)
		{
			CloseSegment(decoder->reader);
			XLogReaderFree(decoder->reader);
		}
		if (decoder->toast != NULL)
			table_close(decoder->toast, AccessShareLock);
		if (decoder->relation != NULL)
			table_close(decoder->relation, AccessShareLock);
		MemoryContextDelete(decoder->memory);
	}
	PG_END_TRY();
	return (Datum) 0;
}
