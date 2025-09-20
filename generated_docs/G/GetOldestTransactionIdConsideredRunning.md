# GetOldestTransactionIdConsideredRunning

## Location
[src/backend/storage/ipc/procarray.c:2034-2046](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L2034-L2046)

## Overview
GetOldestTransactionIdConsideredRunning returns the oldest transaction ID that any currently running backend might still consider as running, used for determining safe truncation points for system structures like pg_subtrans.

## Definition
```c
TransactionId
GetOldestTransactionIdConsideredRunning(void)
```

## Detailed Description
This function provides the most conservative estimate of which transaction IDs might still be referenced by any running backend. Unlike GetOldestNonRemovableTransactionId(), this function is not used for tuple visibility decisions but rather for determining safe truncation boundaries for PostgreSQL's internal data structures.

The primary use cases include:
- Determining how far back pg_subtrans (subtransaction log) can be safely truncated
- Setting safe boundaries for other system catalogs and logs that track transaction state
- Checkpoint and restart point operations that need to ensure system consistency

The function simply computes all visibility horizons and returns the most conservative one (oldest_considered_running), which represents the absolute oldest transaction that any backend might still reference.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - ComputeXidHorizons
  - [ComputeXidHorizonsResult](../C/ComputeXidHorizonsResult.md) (struct type)
- Called from:
  - [CreateCheckPoint](../C/CreateCheckPoint.md)
  - [CreateRestartPoint](../C/CreateRestartPoint.md)

## Notes and Other Information
- This function is specifically designed NOT to be used for visibility/pruning decisions - use GetOldestNonRemovableTransactionId() for those purposes instead
- The returned value is more conservative than visibility horizons because it must account for all possible transaction references, not just tuple visibility requirements
- Critical for maintaining system consistency during log truncation operations
- Used primarily during checkpoint operations to ensure that truncating system logs doesn't remove information that running backends might still need
- The distinction between this function and GetOldestNonRemovableTransactionId() reflects PostgreSQL's careful separation of concerns between tuple visibility (MVCC) and system metadata management