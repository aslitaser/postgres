/*-------------------------------------------------------------------------
 *
 * xidhorizonfuncs.c
 *	  Functions for reporting transaction ID removal horizons.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/xidhorizonfuncs.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <limits.h>

#include "access/transam.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "funcapi.h"
#include "replication/slot.h"
#include "storage/lwlock.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/tuplestore.h"

PG_FUNCTION_INFO_V1(pg_get_xid_horizons);

typedef struct XidHorizonSlot
{
	NameData	name;
	TransactionId xmin;
	TransactionId catalog_xmin;
} XidHorizonSlot;

static int32
xid_age_internal(TransactionId xid)
{
	TransactionId now = GetStableLatestTransactionId();

	if (!TransactionIdIsNormal(xid))
		return INT_MAX;

	return (int32) (now - xid);
}

static void
put_xid_horizon_row(Tuplestorestate *tupstore, TupleDesc tupdesc,
					const char *category, const char *kind,
					TransactionId xid, const char *holder)
{
#define PG_GET_XID_HORIZONS_COLS 5
	Datum		values[PG_GET_XID_HORIZONS_COLS] = {0};
	bool		nulls[PG_GET_XID_HORIZONS_COLS] = {0};
	int			i = 0;

	values[i++] = CStringGetTextDatum(category);
	values[i++] = CStringGetTextDatum(kind);

	if (TransactionIdIsValid(xid))
	{
		values[i++] = TransactionIdGetDatum(xid);
		values[i++] = Int32GetDatum(xid_age_internal(xid));
	}
	else
	{
		nulls[i++] = true;
		nulls[i++] = true;
	}

	if (holder != NULL)
		values[i++] = CStringGetTextDatum(holder);
	else
		nulls[i++] = true;

	Assert(i == PG_GET_XID_HORIZONS_COLS);

	tuplestore_putvalues(tupstore, tupdesc, values, nulls);
}

static int
get_xid_horizon_slots(XidHorizonSlot **slots)
{
	XidHorizonSlot *result;
	int			nslots = 0;

	result = palloc_array(XidHorizonSlot,
						  max_replication_slots + max_repack_replication_slots);

	LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);

	for (int slotno = 0;
		 slotno < max_replication_slots + max_repack_replication_slots;
		 slotno++)
	{
		ReplicationSlot *slot = &ReplicationSlotCtl->replication_slots[slotno];
		ReplicationSlot slot_contents;

		if (!slot->in_use)
			continue;

		SpinLockAcquire(&slot->mutex);
		slot_contents = *slot;
		SpinLockRelease(&slot->mutex);

		result[nslots].name = slot_contents.data.name;
		result[nslots].xmin = slot_contents.data.xmin;
		result[nslots].catalog_xmin = slot_contents.data.catalog_xmin;
		nslots++;
	}

	LWLockRelease(ReplicationSlotControlLock);

	*slots = result;
	return nslots;
}

/*
 * pg_get_xid_horizons - SQL SRF showing transaction ID removal horizons.
 */
Datum
pg_get_xid_horizons(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	XidHorizonsSnapshot horizons;
	XidHorizonSlot *slots;
	PreparedXactXidHolder *prepared_xacts;
	int			nslots;
	int			nprepared_xacts;

	InitMaterializedSRF(fcinfo, 0);

	GetXidHorizonsSnapshot(&horizons);
	nslots = get_xid_horizon_slots(&slots);
	nprepared_xacts = GetPreparedTransactionXidHolders(&prepared_xacts);

	put_xid_horizon_row(rsinfo->setResult, rsinfo->setDesc,
						"horizon", "shared",
						horizons.shared_oldest_nonremovable, NULL);
	put_xid_horizon_row(rsinfo->setResult, rsinfo->setDesc,
						"horizon", "shared_raw",
						horizons.shared_oldest_nonremovable_raw, NULL);
	put_xid_horizon_row(rsinfo->setResult, rsinfo->setDesc,
						"horizon", "data",
						horizons.data_oldest_nonremovable, NULL);
	put_xid_horizon_row(rsinfo->setResult, rsinfo->setDesc,
						"horizon", "catalog",
						horizons.catalog_oldest_nonremovable, NULL);
	put_xid_horizon_row(rsinfo->setResult, rsinfo->setDesc,
						"horizon", "temp",
						horizons.temp_oldest_nonremovable, NULL);
	put_xid_horizon_row(rsinfo->setResult, rsinfo->setDesc,
						"horizon", "oldest_considered_running",
						horizons.oldest_considered_running, NULL);
	put_xid_horizon_row(rsinfo->setResult, rsinfo->setDesc,
						"horizon", "latest_completed",
						XidFromFullTransactionId(horizons.latest_completed),
						NULL);
	put_xid_horizon_row(rsinfo->setResult, rsinfo->setDesc,
						"horizon", "slot_xmin",
						horizons.slot_xmin, NULL);
	put_xid_horizon_row(rsinfo->setResult, rsinfo->setDesc,
						"horizon", "slot_catalog_xmin",
						horizons.slot_catalog_xmin, NULL);

	for (int i = 0; i < horizons.nbackends; i++)
	{
		XidHorizonBackend *backend = &horizons.backends[i];
		char	   *datname = NULL;
		char	   *holder;

		if (OidIsValid(backend->databaseId))
			datname = get_database_name(backend->databaseId);

		holder = psprintf("pid=%d db=%s", backend->pid,
						  datname ? datname : "-");
		put_xid_horizon_row(rsinfo->setResult, rsinfo->setDesc,
							"holder", "backend", backend->xmin, holder);
	}

	for (int i = 0; i < nslots; i++)
	{
		XidHorizonSlot *slot = &slots[i];

		if (TransactionIdIsValid(slot->xmin))
			put_xid_horizon_row(rsinfo->setResult, rsinfo->setDesc,
								"holder", "replication_slot", slot->xmin,
								NameStr(slot->name));

		if (TransactionIdIsValid(slot->catalog_xmin) &&
			!TransactionIdEquals(slot->catalog_xmin, slot->xmin))
			put_xid_horizon_row(rsinfo->setResult, rsinfo->setDesc,
								"holder", "replication_slot_catalog",
								slot->catalog_xmin, NameStr(slot->name));
	}

	for (int i = 0; i < nprepared_xacts; i++)
	{
		PreparedXactXidHolder *prepared_xact = &prepared_xacts[i];

		put_xid_horizon_row(rsinfo->setResult, rsinfo->setDesc,
							"holder", "prepared_xact", prepared_xact->xid,
							prepared_xact->gid);
	}

	return (Datum) 0;
}
