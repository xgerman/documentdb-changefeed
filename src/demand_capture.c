/* Copyright (c) Microsoft Corporation.
 * Licensed under the MIT License
 */
#include "postgres.h"

#include "access/xact.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/snapmgr.h"
#include "utils/timeout.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#include "changefeed.h"

#define FEED_LOCK_PREFIX 0x43463101
#define MAINTENANCE_LOCK_PREFIX 0x43464d01

typedef struct ActivationRequest
{
	Oid database;
	Oid owner;
	Oid relation;
	int caller;
	int errorCode;
	pg_atomic_uint32 status;
	char slot[64];
	char error[512];
} ActivationRequest;

typedef struct OwnedFeed
{
	char *database;
	char *collection;
	Oid relation;
} OwnedFeed;

static MemoryContext OutsideTransactionMemory;

static void
Begin(void)
{
	OutsideTransactionMemory = CurrentMemoryContext;
	StartTransactionCommand();
	enable_timeout_after(STATEMENT_TIMEOUT, 30000);
	PushActiveSnapshot(GetTransactionSnapshot());
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "could not connect to SPI");
}

static void
End(void)
{
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	disable_timeout(STATEMENT_TIMEOUT, false);
	MemoryContextSwitchTo(OutsideTransactionMemory);
}

static void
Execute(const char *query, int count, Oid *types, Datum *values)
{
	if (SPI_execute_with_args(query, count, types, values, NULL, false, 0) < 0)
		elog(ERROR, "changefeed worker query failed");
}

static Datum
Column(int index)
{
	bool isNull;
	Datum value = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc,
							   index, &isNull);
	if (isNull)
		elog(ERROR, "changefeed worker metadata is incomplete");
	return value;
}

static bool
TryLockFeed(Oid relation)
{
	Oid type = INT8OID;
	Datum value = Int64GetDatum(((uint64) FEED_LOCK_PREFIX << 32) | relation);
	Execute("SELECT pg_try_advisory_xact_lock($1)", 1, &type, &value);
	return DatumGetBool(Column(1));
}

static void
WorkerDefinition(BackgroundWorker *worker, const char *function, Datum argument)
{
	memset(worker, 0, sizeof(*worker));
	snprintf(worker->bgw_name, BGW_MAXLEN, "DocumentDB changefeed");
	snprintf(worker->bgw_type, BGW_MAXLEN, "DocumentDB changefeed");
	worker->bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker->bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker->bgw_restart_time = BGW_NEVER_RESTART;
	snprintf(worker->bgw_library_name, MAXPGPATH, "pg_documentdb_changefeed");
	snprintf(worker->bgw_function_name, BGW_MAXLEN, "%s", function);
	worker->bgw_main_arg = argument;
}

void
InitializeDemandCapture(void)
{
	if (process_shared_preload_libraries_in_progress)
	{
		BackgroundWorker worker;
		WorkerDefinition(&worker, "documentdb_changefeed_launcher_main", (Datum) 0);
		worker.bgw_restart_time = 5;
		RegisterBackgroundWorker(&worker);
	}
}

bool
DemandMaintenanceRunning(void)
{
	Oid types[] = {OIDOID, OIDOID};
	Datum values[] = {ObjectIdGetDatum(MyDatabaseId),
					  ObjectIdGetDatum(MAINTENANCE_LOCK_PREFIX)};
	Execute("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory'"
			" AND database=$1 AND classid=$2 AND objid=$1 AND objsubid=1 AND granted)",
			2, types, values);
	return DatumGetBool(Column(1));
}

char *
CreateDemandSlot(Oid relation)
{
	dsm_segment *segment = dsm_create(sizeof(ActivationRequest), 0);
	ActivationRequest *request = dsm_segment_address(segment);
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	pid_t pid;
	char *volatile slot = NULL;

	memset(request, 0, sizeof(*request));
	request->database = MyDatabaseId;
	request->owner = GetUserId();
	request->relation = relation;
	request->caller = MyProcPid;
	pg_atomic_init_u32(&request->status, 0);
	WorkerDefinition(&worker, "documentdb_changefeed_activation_main",
					 UInt32GetDatum(dsm_segment_handle(segment)));
	worker.bgw_notify_pid = MyProcPid;
	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
	{
		dsm_detach(segment);
		ereport(ERROR, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
					   errmsg("changefeed activation requires an available background worker")));
	}
	PG_TRY();
	{
		if (WaitForBackgroundWorkerStartup(handle, &pid) != BGWH_STARTED)
			ereport(ERROR, (errmsg("changefeed activation worker did not start")));
		while (pg_atomic_read_u32(&request->status) == 0)
		{
			ResetLatch(MyLatch);
			CHECK_FOR_INTERRUPTS();
			if (GetBackgroundWorkerPid(handle, &pid) == BGWH_STOPPED &&
				pg_atomic_read_u32(&request->status) == 0)
				ereport(ERROR, (errmsg("changefeed activation worker stopped before readiness")));
			WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					  1000, PG_WAIT_EXTENSION);
		}
		pg_read_barrier();
		if (pg_atomic_read_u32(&request->status) == 2)
			ereport(ERROR, (errcode(request->errorCode), errmsg("%s", request->error)));
		slot = pstrdup(request->slot);
	}
	PG_FINALLY();
	{
		/* A cancelled caller must not kill cleanup or strand an owned slot. */
		dsm_detach(segment);
	}
	PG_END_TRY();
	return slot;
}

void
StartDemandMaintenance(void)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	WorkerDefinition(&worker, "documentdb_changefeed_maintenance_main",
					 ObjectIdGetDatum(MyDatabaseId));
	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		ereport(ERROR, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
					   errmsg("changefeed maintenance requires an available background worker")));
	pfree(handle);
}

static void
DropOwnedSlot(const char *slot)
{
	Oid type = TEXTOID;
	Datum value = CStringGetTextDatum(slot);
	Execute("SELECT 1 FROM pg_replication_slots WHERE slot_name=$1"
			" AND database=current_database() AND plugin='pg_documentdb_changefeed'",
			1, &type, &value);
	if (SPI_processed != 0)
		Execute("SELECT pg_drop_replication_slot($1)", 1, &type, &value);
}

static void
RemoveFeed(text *database, text *collection)
{
	Oid types[] = {TEXTOID, TEXTOID};
	Datum values[] = {PointerGetDatum(database), PointerGetDatum(collection)};
	Execute("SELECT documentdb_changefeed.unregister_feed($1,$2)", 2, types, values);
}

static void
CleanupPending(void)
{
	List *pending = NIL;
	ListCell *cell;
	Begin();
	Execute("SELECT slot,relation_id FROM documentdb_changefeed.pending_slots",
			0, NULL, NULL);
	for (uint64 index = 0; index < SPI_processed; index++)
	{
		bool isNull;
		HeapTuple tuple = SPI_tuptable->vals[index];
		TupleDesc descriptor = SPI_tuptable->tupdesc;
		MemoryContext saved = MemoryContextSwitchTo(OutsideTransactionMemory);
		OwnedFeed *entry = palloc0(sizeof(*entry));
		entry->database = TextDatumGetCString(SPI_getbinval(tuple, descriptor, 1, &isNull));
		entry->relation = DatumGetObjectId(SPI_getbinval(tuple, descriptor, 2, &isNull));
		pending = lappend(pending, entry);
		MemoryContextSwitchTo(saved);
	}
	End();
	foreach(cell, pending)
	{
		OwnedFeed *entry = lfirst(cell);
		Begin();
		if (TryLockFeed(entry->relation))
		{
			Oid type = TEXTOID;
			Datum value = CStringGetTextDatum(entry->database);
			Execute("SELECT 1 FROM documentdb_changefeed.pending_slots WHERE slot=$1",
					1, &type, &value);
			if (SPI_processed != 0)
			{
				DropOwnedSlot(entry->database);
				Execute("DELETE FROM documentdb_changefeed.pending_slots WHERE slot=$1",
						1, &type, &value);
			}
		}
		End();
		pfree(entry->database);
		pfree(entry);
	}
	list_free(pending);
}

static void
Provision(ActivationRequest *request)
{
	char *slot = request->slot;
	uint8 random[8];

	if (!pg_strong_random(random, sizeof(random)))
		elog(ERROR, "could not generate an owned slot name");
	snprintf(slot, sizeof(request->slot), "dcf_%u_%u_", MyDatabaseId, request->relation);
	for (int index = 0, offset = strlen(slot); index < 8; index++)
		snprintf(slot + offset + index * 2, 3, "%02x", random[index]);
	Begin();
	{
		Oid pendingTypes[] = {TEXTOID, OIDOID};
		Datum pendingValues[] = {CStringGetTextDatum(slot), ObjectIdGetDatum(request->relation)};
		Execute("INSERT INTO documentdb_changefeed.pending_slots VALUES ($1,$2)",
				2, pendingTypes, pendingValues);
	}
	End();
	Begin();
	{
		Oid type = TEXTOID;
		Datum value = CStringGetTextDatum(slot);
		Execute("SELECT pg_create_logical_replication_slot($1,'pg_documentdb_changefeed')",
				1, &type, &value);
	}
	End();
}

static bool
AcquireMaintenance(void)
{
	Oid type = INT8OID;
	Datum value = Int64GetDatum(((uint64) MAINTENANCE_LOCK_PREFIX << 32) | MyDatabaseId);
	bool acquired;
	Begin();
	Execute("SELECT pg_try_advisory_lock($1)", 1, &type, &value);
	acquired = DatumGetBool(Column(1));
	End();
	return acquired;
}

static bool
Maintain(void)
{
	List *feeds = NIL;
	ListCell *cell;
	bool any;

	Begin();
	Execute("SELECT to_regclass('documentdb_changefeed.checkpoints') IS NOT NULL", 0, NULL, NULL);
	any = DatumGetBool(Column(1));
	End();
	if (!any)
		return false;
	CleanupPending();
	Begin();
	Execute("SELECT database_name,collection_name,relation_id"
			" FROM documentdb_changefeed.feeds WHERE auto_owned", 0, NULL, NULL);
	any = SPI_processed != 0;
	for (uint64 index = 0; index < SPI_processed; index++)
	{
		bool isNull;
		HeapTuple tuple = SPI_tuptable->vals[index];
		TupleDesc descriptor = SPI_tuptable->tupdesc;
		MemoryContext saved = MemoryContextSwitchTo(OutsideTransactionMemory);
		OwnedFeed *entry = palloc(sizeof(*entry));
		entry->database = TextDatumGetCString(SPI_getbinval(tuple, descriptor, 1, &isNull));
		entry->collection = TextDatumGetCString(SPI_getbinval(tuple, descriptor, 2, &isNull));
		entry->relation = DatumGetObjectId(SPI_getbinval(tuple, descriptor, 3, &isNull));
		feeds = lappend(feeds, entry);
		MemoryContextSwitchTo(saved);
	}
	Execute("SELECT EXISTS(SELECT 1 FROM documentdb_changefeed.pending_slots)", 0, NULL, NULL);
	any = any || DatumGetBool(Column(1));
	End();
	foreach(cell, feeds)
	{
		OwnedFeed *entry = lfirst(cell);
		Oid types[] = {TEXTOID, TEXTOID, INT4OID, OIDOID};
		Datum values[] = {CStringGetTextDatum(entry->database),
						  CStringGetTextDatum(entry->collection), Int32GetDatum(ResumeRetentionSeconds),
						  ObjectIdGetDatum(entry->relation)};
		Begin();
		if (TryLockFeed(entry->relation))
		{
			Execute("SELECT f.slot,COALESCE(f.last_activity <= clock_timestamp()-$3*interval '1 second'"
					" OR c.collection_id IS NULL OR f.relation_id <>"
					" to_regclass('documentdb_data.documents_' || c.collection_id)::oid"
					" OR f.storage_id <> pg_relation_filenode(f.relation_id)"
					" OR s.slot_name IS NULL OR s.wal_status NOT IN ('reserved','extended'),true),"
					" s.confirmed_flush_lsn,pg_current_wal_flush_lsn(),s.restart_lsn"
					" FROM documentdb_changefeed.feeds f"
					" LEFT JOIN documentdb_api_catalog.collections c"
					" ON c.database_name=f.database_name AND c.collection_name=f.collection_name"
					" LEFT JOIN pg_replication_slots s ON s.slot_name=f.slot"
					" WHERE f.database_name=$1 AND f.collection_name=$2 AND f.auto_owned AND f.relation_id=$4",
					4, types, values);
			if (SPI_processed == 1)
			{
				char *slot = TextDatumGetCString(Column(1));
				if (DatumGetBool(Column(2)))
					RemoveFeed(cstring_to_text(entry->database), cstring_to_text(entry->collection));
				else
				{
					XLogRecPtr confirmed = DatumGetLSN(Column(3));
					XLogRecPtr horizon = DatumGetLSN(Column(4));
					XLogRecPtr restart = DatumGetLSN(Column(5));
					XLogRecPtr advance = InvalidXLogRecPtr;
					Execute("SELECT position FROM documentdb_changefeed.checkpoints"
							" WHERE database_name=$1 AND collection_name=$2"
							" AND observed_at <= clock_timestamp()-$3*interval '1 second'"
							" ORDER BY observed_at DESC LIMIT 1", 3, types, values);
					if (SPI_processed == 1)
						advance = DatumGetLSN(Column(1));
					if (horizon - restart > 256 * 1024 * 1024)
						advance = horizon;
					if (!XLogRecPtrIsInvalid(advance) && advance > confirmed)
					{
						Oid advanceTypes[] = {TEXTOID, LSNOID};
						Datum advanceValues[] = {CStringGetTextDatum(slot), LSNGetDatum(advance)};
						Execute("SELECT pg_replication_slot_advance($1,$2)",
								2, advanceTypes, advanceValues);
					}
					Execute("DELETE FROM documentdb_changefeed.checkpoints"
							" WHERE database_name=$1 AND collection_name=$2"
							" AND observed_at <= clock_timestamp()-$3*interval '1 second'",
							3, types, values);
					{
						Oid checkpointTypes[] = {TEXTOID, TEXTOID, LSNOID};
						Datum checkpointValues[] = {values[0], values[1], LSNGetDatum(horizon)};
						Execute("INSERT INTO documentdb_changefeed.checkpoints VALUES ($1,$2,clock_timestamp(),$3)",
								3, checkpointTypes, checkpointValues);
					}
				}
			}
		}
		End();
		pfree(entry->database);
		pfree(entry->collection);
		pfree(entry);
	}
	list_free(feeds);
	return any;
}

static void
Reload(void)
{
	if (ConfigReloadPending)
	{
		ConfigReloadPending = false;
		ProcessConfigFile(PGC_SIGHUP);
	}
}

static void
MaintenanceLoop(void)
{
	for (;;)
	{
		volatile bool any = true;
		MemoryContext cycle = AllocSetContextCreate(TopMemoryContext, "changefeed maintenance",
												   ALLOCSET_DEFAULT_SIZES);
		MemoryContext saved = MemoryContextSwitchTo(cycle);
		PG_TRY();
		{
			any = Maintain();
		}
		PG_CATCH();
		{
			MemoryContextSwitchTo(saved);
			EmitErrorReport();
			AbortCurrentTransaction();
			disable_timeout(STATEMENT_TIMEOUT, false);
			FlushErrorState();
		}
		PG_END_TRY();
		MemoryContextSwitchTo(saved);
		MemoryContextDelete(cycle);
		if (!any)
			return;
		ResetLatch(MyLatch);
		Reload();
		WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
				  Min(60, Max(1, ResumeRetentionSeconds / 4)) * 1000L, PG_WAIT_EXTENSION);
		CHECK_FOR_INTERRUPTS();
		Reload();
	}
}

static void
WorkerSignals(void)
{
	pqsignal(SIGTERM, die);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	BackgroundWorkerUnblockSignals();
	SetConfigOption("lock_timeout", "1000", PGC_USERSET, PGC_S_SESSION);
}

void
documentdb_changefeed_activation_main(Datum argument)
{
	dsm_segment *segment = dsm_attach(DatumGetUInt32(argument));
	ActivationRequest *request;
	volatile bool maintenance = false;
	PGPROC *caller;
	if (segment == NULL)
		proc_exit(1);
	request = dsm_segment_address(segment);
	WorkerSignals();
	BackgroundWorkerInitializeConnectionByOid(request->database, request->owner, 0);
	PG_TRY();
	{
		Provision(request);
		maintenance = AcquireMaintenance();
		pg_write_barrier();
		pg_atomic_write_u32(&request->status, 1);
	}
	PG_CATCH();
	{
		ErrorData *error;
		MemoryContextSwitchTo(TopMemoryContext);
		error = CopyErrorData();
		AbortCurrentTransaction();
		disable_timeout(STATEMENT_TIMEOUT, false);
		FlushErrorState();
		request->errorCode = error->sqlerrcode;
		strlcpy(request->error, error->message, sizeof(request->error));
		FreeErrorData(error);
		pg_write_barrier();
		pg_atomic_write_u32(&request->status, 2);
		/* A reservation makes partial nontransactional slot creation recoverable. */
		maintenance = AcquireMaintenance();
	}
	PG_END_TRY();
	caller = BackendPidGetProc(request->caller);
	if (caller != NULL)
		SetLatch(&caller->procLatch);
	dsm_detach(segment);
	if (maintenance)
		MaintenanceLoop();
	proc_exit(0);
}

void
documentdb_changefeed_maintenance_main(Datum argument)
{
	bool installed;
	WorkerSignals();
	BackgroundWorkerInitializeConnectionByOid(DatumGetObjectId(argument), InvalidOid, 0);
	Begin();
	Execute("SELECT to_regclass('documentdb_changefeed.checkpoints') IS NOT NULL", 0, NULL, NULL);
	installed = DatumGetBool(Column(1));
	End();
	if (installed && AcquireMaintenance())
		MaintenanceLoop();
	proc_exit(0);
}

void
documentdb_changefeed_launcher_main(Datum argument)
{
	WorkerSignals();
	BackgroundWorkerInitializeConnection("postgres", NULL, 0);
	for (;;)
	{
		List *databases = NIL;
		ListCell *cell;
		Oid type = OIDOID;
		Datum value = ObjectIdGetDatum(MAINTENANCE_LOCK_PREFIX);
		Begin();
		Execute("SELECT d.oid FROM pg_database d WHERE d.datallowconn AND NOT d.datistemplate"
				" AND NOT EXISTS(SELECT 1 FROM pg_locks l WHERE l.locktype='advisory'"
				" AND l.database=d.oid AND l.classid=$1 AND l.objid=d.oid AND l.objsubid=1 AND l.granted)",
				1, &type, &value);
		for (uint64 index = 0; index < SPI_processed; index++)
		{
			bool isNull;
			Oid database = DatumGetObjectId(SPI_getbinval(SPI_tuptable->vals[index],
														SPI_tuptable->tupdesc, 1, &isNull));
			MemoryContext saved = MemoryContextSwitchTo(TopMemoryContext);
			databases = lappend_oid(databases, database);
			MemoryContextSwitchTo(saved);
		}
		End();
		foreach(cell, databases)
		{
			BackgroundWorker worker;
			BackgroundWorkerHandle *handle;
			WorkerDefinition(&worker, "documentdb_changefeed_maintenance_main",
							 ObjectIdGetDatum(lfirst_oid(cell)));
			if (!RegisterDynamicBackgroundWorker(&worker, &handle))
				break;
			pfree(handle);
		}
		list_free(databases);
		ResetLatch(MyLatch);
		WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
				  60000, PG_WAIT_EXTENSION);
		CHECK_FOR_INTERRUPTS();
		Reload();
	}
}
