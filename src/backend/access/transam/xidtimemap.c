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
#include "utils/xid8.h"

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
static bool XidTimeMapFindTimeBracket(TimestampTz target,
									  XidTimeSample *lower,
									  XidTimeSample *upper);
static bool XidTimeMapFindXidBracket(FullTransactionId target,
									 XidTimeSample *lower,
									 XidTimeSample *upper);
static FullTransactionId XidTimeMapInterpolateXid(XidTimeSample lower,
												  XidTimeSample upper,
												  TimestampTz target);
static TimestampTz XidTimeMapInterpolateTimestamp(XidTimeSample lower,
												  XidTimeSample upper,
												  FullTransactionId target);
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

Datum
pg_xid_assigned_at(PG_FUNCTION_ARGS)
{
	FullTransactionId target = PG_GETARG_FULLTRANSACTIONID(0);
	XidTimeSample lower;
	XidTimeSample upper;
	TimestampTz result;

	if (!XidTimeMapFindXidBracket(target, &lower, &upper))
		PG_RETURN_NULL();

	result = XidTimeMapInterpolateTimestamp(lower, upper, target);

	PG_RETURN_TIMESTAMPTZ(result);
}

Datum
pg_xid_at_time(PG_FUNCTION_ARGS)
{
	TimestampTz target = PG_GETARG_TIMESTAMPTZ(0);
	XidTimeSample lower;
	XidTimeSample upper;
	FullTransactionId result;

	if (!XidTimeMapFindTimeBracket(target, &lower, &upper))
		PG_RETURN_NULL();

	result = XidTimeMapInterpolateXid(lower, upper, target);

	PG_RETURN_FULLTRANSACTIONID(result);
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
XidTimeMapFindTimeBracket(TimestampTz target, XidTimeSample *lower,
						  XidTimeSample *upper)
{
	XidTimeSample live;
	XidTimeSample prev;
	int			capacity;
	int			count;
	int			i;
	int			oldest;

	live.xid = ReadNextFullTransactionId();
	live.ts = GetCurrentTimestamp();

	LWLockAcquire(XidTimeMapLock, LW_SHARED);

	count = XidTimeMap->count;
	capacity = XidTimeMap->capacity;
	if (count == 0)
	{
		LWLockRelease(XidTimeMapLock);
		return false;
	}

	oldest = (XidTimeMap->head - count + capacity) % capacity;
	prev = XidTimeMap->samples[oldest];

	if (target < prev.ts)
	{
		LWLockRelease(XidTimeMapLock);
		return false;
	}

	for (i = 1; i < count; i++)
	{
		XidTimeSample cur = XidTimeMap->samples[(oldest + i) % capacity];

		if (target <= cur.ts)
		{
			*lower = prev;
			*upper = cur;
			LWLockRelease(XidTimeMapLock);
			return true;
		}

		prev = cur;
	}

	if (live.ts < prev.ts)
		live.ts = prev.ts;

	if (target <= live.ts)
	{
		*lower = prev;
		*upper = live;
		LWLockRelease(XidTimeMapLock);
		return true;
	}

	LWLockRelease(XidTimeMapLock);
	return false;
}

static bool
XidTimeMapFindXidBracket(FullTransactionId target, XidTimeSample *lower,
						 XidTimeSample *upper)
{
	XidTimeSample live;
	XidTimeSample prev;
	int			capacity;
	int			count;
	int			i;
	int			oldest;

	live.xid = ReadNextFullTransactionId();
	live.ts = GetCurrentTimestamp();

	LWLockAcquire(XidTimeMapLock, LW_SHARED);

	count = XidTimeMap->count;
	capacity = XidTimeMap->capacity;
	if (count == 0)
	{
		LWLockRelease(XidTimeMapLock);
		return false;
	}

	oldest = (XidTimeMap->head - count + capacity) % capacity;
	prev = XidTimeMap->samples[oldest];

	if (FullTransactionIdPrecedes(target, prev.xid))
	{
		LWLockRelease(XidTimeMapLock);
		return false;
	}

	for (i = 1; i < count; i++)
	{
		XidTimeSample cur = XidTimeMap->samples[(oldest + i) % capacity];

		if (FullTransactionIdPrecedesOrEquals(target, cur.xid))
		{
			*lower = prev;
			*upper = cur;
			LWLockRelease(XidTimeMapLock);
			return true;
		}

		prev = cur;
	}

	if (live.ts < prev.ts)
		live.ts = prev.ts;

	if (FullTransactionIdPrecedesOrEquals(target, live.xid))
	{
		*lower = prev;
		*upper = live;
		LWLockRelease(XidTimeMapLock);
		return true;
	}

	LWLockRelease(XidTimeMapLock);
	return false;
}

static FullTransactionId
XidTimeMapInterpolateXid(XidTimeSample lower, XidTimeSample upper,
						 TimestampTz target)
{
	uint64		lower_xid = U64FromFullTransactionId(lower.xid);
	uint64		upper_xid = U64FromFullTransactionId(upper.xid);
	double		ratio;
	uint64		result;

	if (target <= lower.ts || upper_xid <= lower_xid)
		return lower.xid;

	if (target >= upper.ts)
		return upper.xid;

	if (upper.ts <= lower.ts)
		return lower.xid;

	ratio = (double) (target - lower.ts) / (double) (upper.ts - lower.ts);
	result = lower_xid + (uint64) ((double) (upper_xid - lower_xid) * ratio);

	if (result < lower_xid)
		result = lower_xid;
	if (result > upper_xid)
		result = upper_xid;

	return FullTransactionIdFromU64(result);
}

static TimestampTz
XidTimeMapInterpolateTimestamp(XidTimeSample lower, XidTimeSample upper,
							   FullTransactionId target)
{
	uint64		lower_xid = U64FromFullTransactionId(lower.xid);
	uint64		upper_xid = U64FromFullTransactionId(upper.xid);
	uint64		target_xid = U64FromFullTransactionId(target);
	double		ratio;
	TimestampTz result;

	if (target_xid <= lower_xid || upper_xid <= lower_xid)
		return lower.ts;

	if (target_xid >= upper_xid)
		return upper.ts;

	ratio = (double) (target_xid - lower_xid) /
		(double) (upper_xid - lower_xid);
	result = lower.ts + (TimestampTz) ((double) (upper.ts - lower.ts) * ratio);

	if (result < lower.ts)
		result = lower.ts;
	if (result > upper.ts)
		result = upper.ts;

	return result;
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
