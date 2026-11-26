#ifndef DOCUMENTDB_CHANGEFEED_H
#define DOCUMENTDB_CHANGEFEED_H

#include "postgres.h"
#include "fmgr.h"

#if PG_VERSION_NUM < 160000 || PG_VERSION_NUM >= 190000
#error "documentdb_changefeed supports PostgreSQL 16, 17 and 18"
#endif

extern PGDLLEXPORT Datum documentdb_changefeed_version(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum documentdb_changefeed_decode_test(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum documentdb_changefeed_register(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum documentdb_changefeed_unregister(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum documentdb_changefeed_aggregate(PG_FUNCTION_ARGS);
extern bool ChangefeedEnabled;
extern bool ChangefeedPreloaded;
extern int ResumeRetentionSeconds;
extern PGDLLEXPORT void documentdb_changefeed_activation_main(Datum argument);
extern PGDLLEXPORT void documentdb_changefeed_maintenance_main(Datum argument);
extern PGDLLEXPORT void documentdb_changefeed_launcher_main(Datum argument);
void InitializeDemandCapture(void);
char *CreateDemandSlot(Oid relation);
void StartDemandMaintenance(void);
bool DemandMaintenanceRunning(void);
extern PGDLLEXPORT Datum documentdb_changefeed_capture_update(PG_FUNCTION_ARGS);
void InitializeUpdateCapture(void);
void FinalizeUpdateCapture(void);
bool UpdateCaptureAvailable(void);

#endif
