# pg_snapshot_xip

## Location
[src/backend/utils/adt/xid8funcs.c:595-639](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xid8funcs.c#L595-L639)

## Overview
Returns the set of in-progress transaction IDs from a PostgreSQL snapshot as a set-returning function, allowing SQL queries to iterate over all transactions that were active when the snapshot was taken.

## Definition

```c
Datum
pg_snapshot_xip(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in set-returning function (SRF) that extracts and returns the array of in-progress transaction IDs from a snapshot. The  (transaction in-progress) array contains all transaction IDs in the range [xmin, xmax) that were still running when the snapshot was created.

This function is particularly useful for understanding snapshot isolation behavior and debugging transaction visibility issues. Unlike the xmin and xmax functions that return single values, this function returns potentially many transaction IDs, one per call, using PostgreSQL's set-returning function protocol.

The function implements the standard SRF pattern with initialization on first call, state management across multiple calls, and proper cleanup. It makes a copy of the snapshot in the multi-call memory context to ensure data persistence across function calls within the same query execution.

In PostgreSQL's visibility rules, transactions listed in the xip array are considered not visible to the snapshot holder, even though their IDs fall within the [xmin, xmax) range.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Argument 0:  - Pointer to the snapshot from which to extract in-progress transaction IDs

## Dependencies
- Functions called/Symbols referenced:
  - : Checks if this is the first call in a set-returning function sequence
  - : Extracts variable-length data (pg_snapshot) from function arguments
  - : Initializes the function call context for SRF
  - : Allocates memory in the function's multi-call context
  - : Gets the total size of a variable-length PostgreSQL datum
  - : Sets up context for each subsequent call
  - : Converts FullTransactionId to PostgreSQL datum format
  - : Returns the next value in the set and continues
  - : Indicates the end of the result set
- Called from (representative examples):
  - No direct callers found (likely called via SQL function calls)

## Notes and Other Information
- This function returns a set of  values (64-bit transaction IDs) rather than legacy 32-bit transaction IDs
- Located in 
- Uses PostgreSQL's set-returning function (SRF) protocol to return multiple values from a single function call
- Creates a copy of the snapshot to ensure data integrity across multiple function calls
- The number of returned values equals the  field of the snapshot
- This is a SQL-callable function that can be used with  syntax
- Essential for analyzing which specific transactions were in-progress at snapshot creation time

## Simplified Source

```c
Datum pg_snapshot_xip(PG_FUNCTION_ARGS) {
    FuncCallContext *fctx;
    pg_snapshot *snap;

    // First call: initialize context and copy snapshot
    if (SRF_IS_FIRSTCALL()) {
        pg_snapshot *arg = (pg_snapshot *) PG_GETARG_VARLENA_P(0);

        fctx = SRF_FIRSTCALL_INIT();

        // Copy snapshot to function's memory context for persistence
        snap = MemoryContextAlloc(fctx->multi_call_memory_ctx, VARSIZE(arg));
        memcpy(snap, arg, VARSIZE(arg));
        fctx->user_fctx = snap;
    }

    // Subsequent calls: return next transaction ID
    fctx = SRF_PERCALL_SETUP();
    snap = fctx->user_fctx;

    if (fctx->call_cntr < snap->nxip) {
        // Return next in-progress transaction ID
        FullTransactionId value = snap->xip[fctx->call_cntr];
        SRF_RETURN_NEXT(fctx, FullTransactionIdGetDatum(value));
    } else {
        // All transaction IDs returned, finish
        SRF_RETURN_DONE(fctx);
    }
}
```