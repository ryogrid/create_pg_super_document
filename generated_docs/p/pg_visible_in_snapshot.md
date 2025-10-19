# pg_visible_in_snapshot

## Location
[src/backend/utils/adt/xid8funcs.c:555-568](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xid8funcs.c#L555-L568)

## Overview
Determines whether a given transaction ID is visible according to a PostgreSQL snapshot, indicating if the transaction was committed and visible at the time the snapshot was taken.

## Definition

```c
Datum
pg_visible_in_snapshot(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in function that checks transaction visibility within a given snapshot. It takes a full transaction ID (xid8) and a snapshot, then determines whether the transaction would have been visible to a transaction using that snapshot.

The function serves as a SQL-accessible wrapper around the internal  function, which implements the core visibility logic. This is part of PostgreSQL's snapshot isolation mechanism, allowing users to query whether specific transactions were visible at particular points in time.

The visibility determination follows PostgreSQL's snapshot isolation rules:
- Transactions committed before the snapshot's xmin are always visible
- Transactions with IDs >= xmax are never visible (not yet committed when snapshot was taken)  
- Transactions in the in-progress list (xip array) are not visible
- All other transactions in the range [xmin, xmax) are visible

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Argument 0:  - The transaction ID to check for visibility
  - Argument 1:  - Pointer to the snapshot against which to check visibility

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts FullTransactionId from function arguments
  - : Extracts variable-length data (pg_snapshot) from function arguments  
  - : Core function that implements the visibility logic
  - : Returns boolean result to PostgreSQL function call framework
- Called from (representative examples):
  - No direct callers found (likely called via SQL function calls)

## Notes and Other Information
- This function is part of the xid8 function family that exposes transaction visibility to SQL users
- The function operates on  (64-bit) rather than legacy 32-bit transaction IDs
- Located in 
- The underlying  function uses binary search optimization for snapshots with many in-progress transactions
- This is a SQL-callable function that can be used in queries to determine transaction visibility relationships

## Simplified Source

```c
Datum pg_visible_in_snapshot(PG_FUNCTION_ARGS) {
    // Get the transaction ID and snapshot from function arguments
    FullTransactionId value = PG_GETARG_FULLTRANSACTIONID(0);
    pg_snapshot *snap = (pg_snapshot *) PG_GETARG_VARLENA_P(1);

    // Use internal function to determine visibility and return result
    PG_RETURN_BOOL(is_visible_fxid(value, snap));
}
```