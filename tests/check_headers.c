#include "postgres.h"
#include "io/bson_core.h"
#include "api_hooks_def.h"

void DocumentdbChangefeedCheckHeaders(void);

void
DocumentdbChangefeedCheckHeaders(void)
{
	IsChangeStreamEnabledAndCompatible available = NULL;
	CreateBsonUpdateTracker_HookType tracker = NULL;

	(void) available;
	(void) tracker;
}
