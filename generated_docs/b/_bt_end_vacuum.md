# _bt_end_vacuum

## Location
[src/backend/access/nbtree/nbtutils.c:4485-4512](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtutils.c#L4485-L4512)

## Overview
Marks a B-tree VACUUM operation as completed and removes the corresponding entry from the shared vacuum tracking array.

## Definition

```c
void
_bt_end_vacuum(Relation rel)
```
## Detailed Description
This function completes the VACUUM tracking lifecycle started by . It searches the shared memory array () for the entry corresponding to the specified relation and removes it, freeing up the slot for future VACUUM operations. The removal is performed efficiently by moving the last array entry to the position of the removed entry and decrementing the count.

The function is designed to be robust and safe:
- Does not complain if no matching entry is found, allowing safe cleanup in error scenarios
- Uses exclusive locking to ensure atomic removal
- Performs efficient O(1) removal by swapping with the last element

This design allows the caller to use PG_TRY blocks around the start_vacuum operation while ensuring cleanup always succeeds.

## Parameters / Member Variables
- : The B-tree index relation for which VACUUM is ending

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire (with BtreeVacuumLock)
  - LWLockRelease
  - [BTOneVacInfo](../B/BTOneVacInfo.md) (structure access)
- Called from (representative examples):
  - [btbulkdelete](btbulkdelete.md)
  - [_bt_end_vacuum_callback](_bt_end_vacuum_callback.md)

## Notes and Other Information
- Deliberately designed not to raise errors if the entry is not found
- Uses array compaction technique (move last element to removed position)
- Must be called for every successful _bt_start_vacuum call to prevent resource leaks
- Protected by BtreeVacuumLock to ensure thread-safe access to shared vacuum info
- The robust error handling makes it suitable for use in error cleanup paths