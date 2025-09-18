# pg_prepared_xact

## Location
[src/backend/access/transam/twophase.c:711-799](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L711-L799)

## Overview
pg_prepared_xact is a PostgreSQL built-in function that provides a system view showing all currently prepared transactions in the database cluster.

## Definition


## Detailed Description
pg_prepared_xact is a Set-Returning Function (SRF) that implements the pg_prepared_xacts system view. It retrieves information about all prepared transactions and formats it into a structured result set with 5 columns: transaction ID, global ID (GID), preparation timestamp, owner ID, and database ID. The function uses PostgreSQL's SRF framework to return multiple rows, with each row representing one prepared transaction. It calls GetPreparedTransactionList to obtain transaction data and filters out invalid transactions before returning results.

## Parameters / Member Variables
- No input parameters (uses PG_FUNCTION_ARGS macro for PostgreSQL function interface)
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
  - GlobalTransaction (transaction data)
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