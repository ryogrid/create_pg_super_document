# pg_prepared_xact

## Location
[src/backend/access/transam/twophase.c:711-799](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L711-L799)

## Overview
pg_prepared_xact is a PostgreSQL built-in function that provides a system view showing all currently prepared transactions in the database cluster.

## Definition

```c
Datum
pg_prepared_xact(PG_FUNCTION_ARGS)
```
## Detailed Description
pg_prepared_xact is a Set-Returning Function (SRF) that implements the pg_prepared_xacts system view. It retrieves information about all prepared transactions and formats it into a structured result set with 5 columns: transaction ID, global ID (GID), preparation timestamp, owner ID, and database ID. The function uses PostgreSQL's SRF framework to return multiple rows, with each row representing one prepared transaction. It calls GetPreparedTransactionList to obtain transaction data and filters out invalid transactions before returning results.

## Parameters / Member Variables
- Returns: Datum containing tuple data for each prepared transaction row
- Internal Working_State structure contains:
  - : Number of prepared transactions
  - : Array of GlobalTransaction copies  
  - : Current iteration index

## Dependencies
- Functions called/Symbols referenced:
  - [GetPreparedTransactionList](../G/GetPreparedTransactionList.md) (to retrieve transaction list)
  - SRF_IS_FIRSTCALL, SRF_FIRSTCALL_INIT, SRF_PERCALL_SETUP (SRF framework)
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md), TupleDescInitEntry, BlessTupleDesc (tuple descriptor creation)
  - GetPGProcByNumber (to get process information)
  - [TransactionIdGetDatum](../T/TransactionIdGetDatum.md), CStringGetTextDatum, TimestampTzGetDatum, ObjectIdGetDatum (data conversion)
  - [heap_form_tuple](../h/heap_form_tuple.md), HeapTupleGetDatum (tuple creation)
- Data structures accessed:
  - [FuncCallContext](../F/FuncCallContext.md) (SRF context)
  - [GlobalTransaction](../G/GlobalTransaction.md) (transaction data)
  - [PGPROC](../P/PGPROC.md) (process information)
- Called from:
  - SQL queries on pg_prepared_xacts system view

## Notes and Other Information
- Implements the backend for the pg_prepared_xacts system view
- Returns 5 columns: transaction, gid, prepared, ownerid, dbid
- Tuple descriptor must match the pg_prepared_xacts view definition in system_views.sql
- Filters out invalid transactions (gxact->valid check)
- Uses memory context switching for proper memory management across function calls
- Part of PostgreSQL's two-phase commit monitoring infrastructure
- No direct callers since it's registered as a system function accessible via SQL

## Simplified Source

```c
Datum
pg_prepared_xact(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    Working_State *status;

    if (SRF_IS_FIRSTCALL()) {
        // Initialize function context for multiple calls
        funcctx = SRF_FIRSTCALL_INIT();

        // Switch to multi-call memory context
        MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        // Build tuple descriptor with 5 columns: transaction, gid, prepared, ownerid, dbid
        TupleDesc tupdesc = CreateTemplateTupleDesc(5);
        TupleDescInitEntry(tupdesc, 1, "transaction", XIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, 2, "gid", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 3, "prepared", TIMESTAMPTZOID, -1, 0);
        TupleDescInitEntry(tupdesc, 4, "ownerid", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, 5, "dbid", OIDOID, -1, 0);
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        // Get list of all prepared transactions
        status = palloc(sizeof(Working_State));
        funcctx->user_fctx = status;
        status->ngxacts = GetPreparedTransactionList(&status->array);
        status->currIdx = 0;

        MemoryContextSwitchTo(oldcontext);
    }

    // Set up for each call
    funcctx = SRF_PERCALL_SETUP();
    status = funcctx->user_fctx;

    // Return one row for each valid prepared transaction
    while (status->array != NULL && status->currIdx < status->ngxacts) {
        GlobalTransaction gxact = &status->array[status->currIdx++];

        if (!gxact->valid)
            continue;

        // Get process info and build result tuple
        PGPROC *proc = GetPGProcByNumber(gxact->pgprocno);
        Datum values[5] = {
            TransactionIdGetDatum(proc->xid),
            CStringGetTextDatum(gxact->gid),
            TimestampTzGetDatum(gxact->prepared_at),
            ObjectIdGetDatum(gxact->owner),
            ObjectIdGetDatum(proc->databaseId)
        };
        bool nulls[5] = {0};

        HeapTuple tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(funcctx);
}
```