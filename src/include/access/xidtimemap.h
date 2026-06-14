/*-------------------------------------------------------------------------
 *
 * xidtimemap.h
 *	  transaction ID assignment time map
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/include/access/xidtimemap.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef XIDTIMEMAP_H
#define XIDTIMEMAP_H

#include "access/transam.h"
#include "datatype/timestamp.h"
#include "storage/shmem.h"

typedef struct XidTimeSample
{
	FullTransactionId xid;
	TimestampTz ts;
} XidTimeSample;

extern PGDLLIMPORT int xid_time_map_samples;
extern PGDLLIMPORT int xid_time_map_interval;

extern const ShmemCallbacks XidTimeMapShmemCallbacks;

extern Size XidTimeMapShmemSize(void);
extern void XidTimeMapShmemInit(void);
extern void XidTimeMapMaybeSample(FullTransactionId xid);

#endif							/* XIDTIMEMAP_H */
