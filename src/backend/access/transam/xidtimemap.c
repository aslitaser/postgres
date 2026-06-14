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

#include <fcntl.h>
#include <unistd.h>

#include "access/xact.h"
#include "access/xidtimemap.h"
#include "storage/fd.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/subsystems.h"
#include "utils/timestamp.h"
#include "utils/xid8.h"

int			xid_time_map_samples = 8192;
int			xid_time_map_interval = 1000;
int			max_standby_feedback_lag = 0;

#define XID_TIME_MAP_FILE		"pg_xid_time_map"
#define XID_TIME_MAP_TMP_FILE	"pg_xid_time_map.tmp"
#define XID_TIME_MAP_MAGIC		0x5849544D	/* "XITM" */
#define XID_TIME_MAP_VERSION	1

typedef struct XidTimeMapCtl
{
	int			capacity;
	int			head;
	int			count;
	TimestampTz last_sample_ts;
	XidTimeSample samples[FLEXIBLE_ARRAY_MEMBER];
} XidTimeMapCtl;

typedef struct XidTimeMapFileHeader
{
	uint32		magic;
	uint32		version;
	int32		capacity;
	int32		head;
	int32		count;
	TimestampTz last_sample_ts;
} XidTimeMapFileHeader;

static XidTimeMapCtl *XidTimeMap = NULL;

static void XidTimeMapShmemRequest(void *arg);
static void XidTimeMapShmemInitCallback(void *arg);
static void XidTimeMapInitEmpty(const char *reason);
static void XidTimeMapLoad(void);
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
static bool XidTimeMapReadExact(int fd, void *buffer, Size size);
static bool XidTimeMapSampleDue(TimestampTz last_sample_ts,
								TimestampTz now);
static bool XidTimeMapWriteExact(int fd, const void *buffer, Size size,
								 const char *path);
static FullTransactionId XidTimeMapXidAtTime(TimestampTz target, bool *found);

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

	XidTimeMapLoad();
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
	FullTransactionId result;
	bool		found;

	result = XidTimeMapXidAtTime(target, &found);
	if (!found)
		PG_RETURN_NULL();

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

FullTransactionId
XidTimeMapBoundXid(void)
{
	TimestampTz bound_time;
	FullTransactionId bound_xid;
	bool		found;

	if (max_standby_feedback_lag <= 0)
		return InvalidFullTransactionId;

	bound_time = TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
											 -max_standby_feedback_lag);
	bound_xid = XidTimeMapXidAtTime(bound_time, &found);
	if (!found)
		return InvalidFullTransactionId;

	return bound_xid;
}

void
CheckPointXidTimeMap(void)
{
	char	   *buffer;
	XidTimeMapFileHeader *header;
	XidTimeSample *samples;
	Size		samples_size;
	Size		file_size;
	int			fd;

	Assert(XidTimeMap != NULL);

	samples_size = mul_size((Size) XidTimeMap->capacity,
							sizeof(XidTimeSample));
	file_size = add_size(sizeof(XidTimeMapFileHeader), samples_size);
	buffer = palloc(file_size);
	header = (XidTimeMapFileHeader *) buffer;
	samples = (XidTimeSample *) (buffer + sizeof(XidTimeMapFileHeader));

	LWLockAcquire(XidTimeMapLock, LW_SHARED);

	header->magic = XID_TIME_MAP_MAGIC;
	header->version = XID_TIME_MAP_VERSION;
	header->capacity = XidTimeMap->capacity;
	header->head = XidTimeMap->head;
	header->count = XidTimeMap->count;
	header->last_sample_ts = XidTimeMap->last_sample_ts;
	memcpy(samples, XidTimeMap->samples, samples_size);

	LWLockRelease(XidTimeMapLock);

	fd = OpenTransientFile(XID_TIME_MAP_TMP_FILE,
						   O_CREAT | O_TRUNC | O_WRONLY | PG_BINARY);
	if (fd < 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m",
						XID_TIME_MAP_TMP_FILE)));
		pfree(buffer);
		return;
	}

	if (!XidTimeMapWriteExact(fd, buffer, file_size,
							  XID_TIME_MAP_TMP_FILE))
	{
		CloseTransientFile(fd);
		unlink(XID_TIME_MAP_TMP_FILE);
		pfree(buffer);
		return;
	}

	if (pg_fsync(fd) != 0)
	{
		int			save_errno = errno;

		CloseTransientFile(fd);
		unlink(XID_TIME_MAP_TMP_FILE);
		pfree(buffer);
		errno = save_errno;
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m",
						XID_TIME_MAP_TMP_FILE)));
		return;
	}

	if (CloseTransientFile(fd) != 0)
	{
		int			save_errno = errno;

		unlink(XID_TIME_MAP_TMP_FILE);
		pfree(buffer);
		errno = save_errno;
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m",
						XID_TIME_MAP_TMP_FILE)));
		return;
	}

	if (durable_rename(XID_TIME_MAP_TMP_FILE, XID_TIME_MAP_FILE, LOG) < 0)
	{
		unlink(XID_TIME_MAP_TMP_FILE);
		pfree(buffer);
		return;
	}

	pfree(buffer);
}

static void
XidTimeMapInitEmpty(const char *reason)
{
	XidTimeMap->capacity = xid_time_map_samples;
	XidTimeMap->head = 0;
	XidTimeMap->count = 0;
	XidTimeMap->last_sample_ts = 0;
	MemSet(XidTimeMap->samples, 0,
		   (Size) XidTimeMap->capacity * sizeof(XidTimeSample));

	if (reason)
		elog(LOG, "xid time map started empty with %d sample slots: %s",
			 XidTimeMap->capacity, reason);
}

static void
XidTimeMapLoad(void)
{
	XidTimeMapFileHeader header;
	XidTimeSample *samples;
	Size		samples_size;
	int			fd;

	XidTimeMapInitEmpty(NULL);

	fd = OpenTransientFile(XID_TIME_MAP_FILE, O_RDONLY | PG_BINARY);
	if (fd < 0)
	{
		if (errno == ENOENT)
			XidTimeMapInitEmpty("state file not found");
		else
			XidTimeMapInitEmpty("could not open state file");
		return;
	}

	if (!XidTimeMapReadExact(fd, &header, sizeof(XidTimeMapFileHeader)))
	{
		CloseTransientFile(fd);
		XidTimeMapInitEmpty("state file is too short");
		return;
	}

	if (header.magic != XID_TIME_MAP_MAGIC ||
		header.version != XID_TIME_MAP_VERSION)
	{
		CloseTransientFile(fd);
		XidTimeMapInitEmpty("state file has invalid magic or version");
		return;
	}

	if (header.capacity != xid_time_map_samples ||
		header.head < 0 || header.head >= header.capacity ||
		header.count < 0 || header.count > header.capacity)
	{
		CloseTransientFile(fd);
		XidTimeMapInitEmpty("state file does not match current configuration");
		return;
	}

	samples_size = mul_size((Size) header.capacity, sizeof(XidTimeSample));
	samples = palloc(samples_size);
	if (!XidTimeMapReadExact(fd, samples, samples_size))
	{
		CloseTransientFile(fd);
		pfree(samples);
		XidTimeMapInitEmpty("state file is too short");
		return;
	}

	if (CloseTransientFile(fd) != 0)
	{
		pfree(samples);
		XidTimeMapInitEmpty("could not close state file");
		return;
	}

	XidTimeMap->capacity = header.capacity;
	XidTimeMap->head = header.head;
	XidTimeMap->count = header.count;
	XidTimeMap->last_sample_ts = header.last_sample_ts;
	memcpy(XidTimeMap->samples, samples, samples_size);
	pfree(samples);

	elog(LOG, "xid time map restored %d samples from \"%s\"",
		 XidTimeMap->count, XID_TIME_MAP_FILE);
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

static bool
XidTimeMapReadExact(int fd, void *buffer, Size size)
{
	char	   *ptr = buffer;
	Size		remaining = size;

	while (remaining > 0)
	{
		ssize_t		written;

		written = read(fd, ptr, remaining);
		if (written < 0)
		{
			if (errno == EINTR)
				continue;
			return false;
		}
		if (written == 0)
			return false;

		ptr += written;
		remaining -= written;
	}

	return true;
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

static bool
XidTimeMapWriteExact(int fd, const void *buffer, Size size, const char *path)
{
	const char *ptr = buffer;
	Size		remaining = size;

	while (remaining > 0)
	{
		ssize_t		written;

		errno = 0;
		written = write(fd, ptr, remaining);
		if (written < 0)
		{
			if (errno == EINTR)
				continue;

			ereport(LOG,
					(errcode_for_file_access(),
					 errmsg("could not write to file \"%s\": %m", path)));
			return false;
		}
		if (written == 0)
		{
			errno = ENOSPC;
			ereport(LOG,
					(errcode_for_file_access(),
					 errmsg("could not write to file \"%s\": %m", path)));
			return false;
		}

		ptr += written;
		remaining -= written;
	}

	return true;
}

static FullTransactionId
XidTimeMapXidAtTime(TimestampTz target, bool *found)
{
	XidTimeSample lower;
	XidTimeSample upper;

	if (!XidTimeMapFindTimeBracket(target, &lower, &upper))
	{
		*found = false;
		return InvalidFullTransactionId;
	}

	*found = true;
	return XidTimeMapInterpolateXid(lower, upper, target);
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
