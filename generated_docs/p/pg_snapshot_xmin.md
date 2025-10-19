# pg_snapshot_xmin

## Location
[src/backend/utils/adt/xid8funcs.c:569-581](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xid8funcs.c#L569-L581)

## Overview
Extracts and returns the minimum transaction ID (xmin) from a PostgreSQL snapshot, representing the earliest transaction that was still active when the snapshot was taken.

## Definition

```c
Datum
pg_snapshot_xmin(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in function that extracts the  field from a snapshot. The  value represents the minimum transaction ID that was still running when the snapshot was created. This is a crucial component of PostgreSQL's snapshot isolation mechanism.

In snapshot isolation, the  serves as a visibility boundary: any transaction with an ID less than  is guaranteed to have been committed (or aborted) before the snapshot was taken and is therefore visible to transactions using this snapshot. This helps implement MVCC (Multi-Version Concurrency Control) by defining the lower bound of the visibility window.

The function is part of the xid8 function family that allows SQL users to inspect and work with transaction snapshots programmatically.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Argument 0:  - Pointer to the snapshot from which to extract the xmin value

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts variable-length data (pg_snapshot) from function arguments
  - : Returns FullTransactionId result to PostgreSQL function call framework
- Called from (representative examples):
  - No direct callers found (likely called via SQL function calls)

## Notes and Other Information
- This function provides SQL access to the  field of PostgreSQL snapshots
- The returned value is a  (64-bit transaction ID) rather than the legacy 32-bit format
- Located in 
- The  boundary is essential for garbage collection - rows with xmin less than the oldest active snapshot's xmin can potentially be vacuumed
- This is a SQL-callable function that can be used in queries to analyze snapshot characteristics and transaction visibility ranges

## Simplified Source

```c
Datum
pg_snapshot_xmin(PG_FUNCTION_ARGS)
{
    // Extract snapshot from function arguments
    pg_snapshot *snap = (pg_snapshot *) PG_GETARG_VARLENA_P(0);

    // Return the xmin field from the snapshot
    PG_RETURN_FULLTRANSACTIONID(snap->xmin);
}
```