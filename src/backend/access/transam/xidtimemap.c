/*-------------------------------------------------------------------------
 *
 * xidtimemap.c
 *	  transaction ID assignment time map
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/access/transam/xidtimemap.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xidtimemap.h"
#include "storage/shmem.h"
#include "storage/subsystems.h"

int			xid_time_map_samples = 8192;

typedef struct XidTimeMapCtl
{
	int			capacity;
	int			head;
	int			count;
	XidTimeSample samples[FLEXIBLE_ARRAY_MEMBER];
} XidTimeMapCtl;

static XidTimeMapCtl *XidTimeMap = NULL;

static void XidTimeMapShmemRequest(void *arg);
static void XidTimeMapShmemInitCallback(void *arg);

const ShmemCallbacks XidTimeMapShmemCallbacks = {
	.request_fn = XidTimeMapShmemRequest,
	.init_fn = XidTimeMapShmemInitCallback,
};

Size
XidTimeMapShmemSize(void)
{
	Size		size;

	size = offsetof(XidTimeMapCtl, samples);
	size = add_size(size,
					mul_size((Size) xid_time_map_samples,
							 sizeof(XidTimeSample)));

	return size;
}

void
XidTimeMapShmemInit(void)
{
	Assert(XidTimeMap != NULL);

	XidTimeMap->capacity = xid_time_map_samples;
	XidTimeMap->head = 0;
	XidTimeMap->count = 0;

	elog(LOG, "xid time map initialized with %d sample slots",
		 XidTimeMap->capacity);
}

static void
XidTimeMapShmemRequest(void *arg)
{
	ShmemRequestStruct(.name = "XID time map",
					   .size = XidTimeMapShmemSize(),
					   .ptr = (void **) &XidTimeMap);
}

static void
XidTimeMapShmemInitCallback(void *arg)
{
	XidTimeMapShmemInit();
}
