# _bt_start_vacuum

## Location
[src/backend/access/nbtree/nbtutils.c:4428-4484](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtutils.c#L4428-L4484)

## Overview
Assigns a cycle ID to a B-tree index at the beginning of a VACUUM operation, tracking active VACUUM processes to coordinate concurrent operations.

## Definition

```c
BTCycleId
_bt_start_vacuum(Relation rel)
```
## Detailed Description
This function initiates VACUUM tracking for a B-tree index by assigning a unique cycle ID that will be used throughout the VACUUM process. The cycle ID serves to identify pages that were split during VACUUM, allowing VACUUM to detect concurrent page modifications. The function maintains a shared memory array () that tracks all currently active VACUUM operations across different indexes.

The function implements several safety mechanisms:
- Ensures no duplicate VACUUM operations on the same index
- Assigns cycle IDs in a safe range (1 to MAX_BT_CYCLE_ID)
- Uses exclusive locking to prevent race conditions
- Provides error handling with explicit lock release before throwing errors

The caller must guarantee that  will eventually be called to prevent permanent resource leaks, typically using .

## Parameters / Member Variables
- : The B-tree index relation for which VACUUM is starting

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire (with BtreeVacuumLock)
  - LWLockRelease
  - RelationGetRelationName
  - elog
- Called from (representative examples):
  - [btbulkdelete](btbulkdelete.md)

## Notes and Other Information
- The cycle ID is a 16-bit unsigned integer (BTCycleId = uint16)
- Cycle IDs are assigned sequentially, wrapping around after MAX_BT_CYCLE_ID
- Zero and values > MAX_BT_CYCLE_ID are reserved and avoided
- The function checks for existing VACUUM operations on the same relation to prevent conflicts
- Resource management is critical - failure to call _bt_end_vacuum causes permanent slot leakage
- Uses BtreeVacuumLock for synchronization across all B-tree VACUUM operations