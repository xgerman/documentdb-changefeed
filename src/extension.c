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
static bool (**AvailabilityHook)(void) = NULL;

void _PG_init(void);
void _PG_fini(void);

static bool
Available(void)
{
	return ChangefeedEnabled && !RecoveryInProgress() &&
		   OidIsValid(get_extension_oid("documentdb_changefeed", true));
}

void
_PG_init(void)
{
	DefineCustomBoolVariable("documentdb_changefeed.enable",
							 "Enable the experimental collection changefeed provider.",
							 NULL, &ChangefeedEnabled, false, PGC_SUSET, 0,
							 NULL, NULL, NULL);
	AvailabilityHook = dlsym(RTLD_DEFAULT, "is_changestream_enabled_and_compatible_hook");
	if (AvailabilityHook != NULL)
	{
		if (*AvailabilityHook != NULL && *AvailabilityHook != Available)
			ereport(ERROR,
					(errmsg("a change stream provider is already registered")));
		*AvailabilityHook = Available;
	}
}

void
_PG_fini(void)
{
	if (AvailabilityHook != NULL && *AvailabilityHook == Available)
		*AvailabilityHook = NULL;
}

Datum
documentdb_changefeed_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text("0.1.0-dev"));
}
