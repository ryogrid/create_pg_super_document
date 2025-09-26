# RelationExtensionLockWaiterCount

## Location
[src/backend/storage/lmgr/lmgr.c:455-469](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lmgr.c#L455-L469)

## Overview
Counts the number of processes currently waiting for a relation extension lock on the specified relation.

## Definition

```c
int
RelationExtensionLockWaiterCount(Relation relation)
```
## Detailed Description
This function provides visibility into lock contention for relation extension operations by counting how many processes are currently waiting to acquire an extension lock on a given relation. It creates the same lock tag used by other relation extension locking functions and queries the lock manager to determine the number of waiters.

This information is valuable for monitoring system performance and understanding contention patterns, particularly in scenarios where multiple processes frequently attempt to extend the same relation simultaneously. The function helps identify potential bottlenecks in relation growth operations.

## Parameters / Member Variables
- `relation`: Pointer to the Relation structure for which to count extension lock waiters

## Return Value
- Returns an integer representing the number of processes waiting for the relation extension lock
- Returns 0 if no processes are waiting or if no lock exists for the relation

## Dependencies
- Functions called/Symbols referenced:
  - SET_LOCKTAG_RELATION_EXTEND (macro to set up lock tag for relation extension)
  - [LockWaiterCount](../L/LockWaiterCount.md) (core function to count waiters for a specific lock tag)
- Called from (representative examples):
  - MAX_BUFFERS_TO_EXTEND_BY (heap input/output operations for buffer extension decisions)
  - [XLTW_Oper](../X/XLTW_Oper.md) (transaction lock wait operations)

## Notes and Other Information
- Used primarily for performance monitoring and debugging lock contention issues
- Helpful in implementing adaptive algorithms that adjust behavior based on lock contention levels
- The function provides a snapshot of waiter count at the time of the call; the actual count may change immediately after
- Commonly used in heap extension logic to determine optimal buffer extension strategies
- Does not include the current lock holder in the count, only waiting processes
- The underlying LockWaiterCount function uses exclusive lightweight locks to ensure consistent results