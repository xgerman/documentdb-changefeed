#include "postgres.h"
#include <dlfcn.h>
#include "access/xlog.h"
#include "commands/extension.h"
#include "miscadmin.h"
#include "utils/guc.h"
#include "utils/builtins.h"

#include "changefeed.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(documentdb_changefeed_version);

bool ChangefeedEnabled = false;
bool ChangefeedPreloaded = false;
int ResumeRetentionSeconds = 24 * 60 * 60;
static bool (**AvailabilityHook)(void) = NULL;

void _PG_init(void);
void _PG_fini(void);

static bool
Available(void)
{
	return ChangefeedEnabled && ChangefeedPreloaded && !RecoveryInProgress() &&
		   OidIsValid(get_extension_oid("documentdb_changefeed", true));
}

void
_PG_init(void)
{
	ChangefeedPreloaded = process_shared_preload_libraries_in_progress;
	DefineCustomBoolVariable("documentdb_changefeed.enable",
							 "Enable the experimental collection changefeed provider.",
							 NULL, &ChangefeedEnabled, true, PGC_SUSET, 0,
							 NULL, NULL, NULL);
	DefineCustomIntVariable("documentdb_changefeed.resume_retention",
							"Maximum retained history and idle capture grace period.",
							NULL, &ResumeRetentionSeconds, 24 * 60 * 60,
							1, 24 * 60 * 60, PGC_SIGHUP, GUC_UNIT_S,
							NULL, NULL, NULL);
	AvailabilityHook = dlsym(RTLD_DEFAULT, "is_changestream_enabled_and_compatible_hook");
	if (AvailabilityHook != NULL)
	{
		if (*AvailabilityHook != NULL && *AvailabilityHook != Available)
			ereport(ERROR,
					(errmsg("a change stream provider is already registered")));
	}
	InitializeUpdateCapture();
	InitializeDemandCapture();
	if (AvailabilityHook != NULL)
		*AvailabilityHook = Available;
}

void
_PG_fini(void)
{
	FinalizeUpdateCapture();
	if (AvailabilityHook != NULL && *AvailabilityHook == Available)
		*AvailabilityHook = NULL;
}

Datum
documentdb_changefeed_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text("0.2.0-dev"));
}
