# AtSubAbort_Snapshot

## Location
[src/backend/utils/time/snapmgr.c:959-994](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/snapmgr.c#L959-L994)

## Overview
Cleans up active snapshots and releases associated resources when a subtransaction is aborted, ensuring proper memory management and snapshot reference counting.

## Definition

```c
void
AtSubAbort_Snapshot(int level)
```
## Detailed Description
This function is called during subtransaction abort to clean up all active snapshots that were created at or above the specified subtransaction level. It performs a complete cleanup by:

1. Iterating through the active snapshot stack from the top
2. Removing all snapshots with levels >= the aborting subtransaction level
3. Properly decrementing reference counts (active_count) for each removed snapshot
4. Freeing snapshot memory when both active_count and regd_count reach zero
5. Updating global snapshot tracking variables (ActiveSnapshot, OldestActiveSnapshot)
6. Calling SnapshotResetXmin() to potentially advance the process's xmin value

The function ensures that aborted subtransactions do not leave dangling snapshot references and that memory is properly reclaimed when snapshots are no longer needed by any transaction level.

## Parameters / Member Variables
- : The subtransaction level being aborted (all snapshots at this level and above will be cleaned up)

## Dependencies
- Functions called/Symbols referenced:
  - ActiveSnapshotElt
  - FreeSnapshot
  - [SnapshotResetXmin](../S/SnapshotResetXmin.md)
- Called from (representative examples):
  - [AbortSubTransaction](AbortSubTransaction.md)
  - IsMVCCSnapshot (via header inclusion)

## Notes and Other Information
- Part of PostgreSQL's nested transaction (savepoint) error handling
- Maintains proper reference counting through active_count and regd_count fields
- Only frees snapshot memory when all references are gone (both active and registered counts are zero)
- Updates both ActiveSnapshot and OldestActiveSnapshot global pointers
- Always calls SnapshotResetXmin() at the end to optimize xmin for garbage collection
- Critical for preventing memory leaks during subtransaction rollback scenarios
- Handles the case where ActiveSnapshot becomes NULL by also clearing OldestActiveSnapshot