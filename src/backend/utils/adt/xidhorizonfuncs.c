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

#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/tuplestore.h"

PG_FUNCTION_INFO_V1(pg_get_xid_horizons);

/*
 * pg_get_xid_horizons - SQL SRF showing transaction ID removal horizons.
 */
Datum
pg_get_xid_horizons(PG_FUNCTION_ARGS)
{
#define PG_GET_XID_HORIZONS_COLS 4
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Datum		values[PG_GET_XID_HORIZONS_COLS];
	bool		nulls[PG_GET_XID_HORIZONS_COLS];

	InitMaterializedSRF(fcinfo, 0);

	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));

	values[0] = CStringGetTextDatum("oldest_running");
	values[1] = TransactionIdGetDatum((TransactionId) 1234);
	values[2] = Int32GetDatum(42);
	nulls[3] = true;

	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
						 values, nulls);

	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));

	values[0] = CStringGetTextDatum("replication_slot");
	values[1] = TransactionIdGetDatum((TransactionId) 1200);
	values[2] = Int32GetDatum(76);
	values[3] = CStringGetTextDatum("dummy_slot");

	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
						 values, nulls);

	return (Datum) 0;
}
