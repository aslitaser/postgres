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

#include "access/xact.h"
#include "access/xidtimemap.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/subsystems.h"
#include "utils/timestamp.h"

int			xid_time_map_samples = 8192;
int			xid_time_map_interval = 1000;

typedef struct XidTimeMapCtl
{
	int			capacity;
	int			head;
	int			count;
	TimestampTz last_sample_ts;
	XidTimeSample samples[FLEXIBLE_ARRAY_MEMBER];
} XidTimeMapCtl;

static XidTimeMapCtl *XidTimeMap = NULL;

static void XidTimeMapShmemRequest(void *arg);
static void XidTimeMapShmemInitCallback(void *arg);
static bool XidTimeMapSampleDue(TimestampTz last_sample_ts,
								TimestampTz now);

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
	XidTimeMap->last_sample_ts = 0;

	elog(LOG, "xid time map initialized with %d sample slots",
		 XidTimeMap->capacity);
}

void
XidTimeMapMaybeSample(FullTransactionId xid)
{
	TimestampTz now;
	TimestampTz last_sample_ts;
	int			head;
	int			next_head;
	int			count;

	Assert(XidTimeMap != NULL);

	now = GetCurrentTransactionStartTimestamp();
	last_sample_ts = XidTimeMap->last_sample_ts;

	if (!XidTimeMapSampleDue(last_sample_ts, now))
		return;

	LWLockAcquire(XidTimeMapLock, LW_EXCLUSIVE);

	last_sample_ts = XidTimeMap->last_sample_ts;
	if (!XidTimeMapSampleDue(last_sample_ts, now))
	{
		LWLockRelease(XidTimeMapLock);
		return;
	}

	if (now < last_sample_ts)
		now = last_sample_ts;

	head = XidTimeMap->head;
	XidTimeMap->samples[head].xid = xid;
	XidTimeMap->samples[head].ts = now;

	XidTimeMap->head = (head + 1) % XidTimeMap->capacity;
	next_head = XidTimeMap->head;
	if (XidTimeMap->count < XidTimeMap->capacity)
		XidTimeMap->count++;
	XidTimeMap->last_sample_ts = now;
	count = XidTimeMap->count;

	LWLockRelease(XidTimeMapLock);

	elog(LOG, "xid time map sampled xid " UINT64_FORMAT " count %d head %d",
		 U64FromFullTransactionId(xid), count, next_head);
}

static bool
XidTimeMapSampleDue(TimestampTz last_sample_ts, TimestampTz now)
{
	if (xid_time_map_interval == 0 || last_sample_ts == 0)
		return true;

	if (now < last_sample_ts)
		return true;

	return TimestampDifferenceExceeds(last_sample_ts, now,
									  xid_time_map_interval);
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
