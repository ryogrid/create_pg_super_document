# KnownAssignedXidsReset

## Location
[src/backend/storage/ipc/procarray.c:5255-5266](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L5255-L5266)

## Overview
KnownAssignedXidsReset clears the KnownAssignedXids array by resetting all its metadata counters and position indicators to zero.

## Definition

```c
static void
KnownAssignedXidsReset(void)
```
## Detailed Description
This function provides a complete reset of the KnownAssignedXids data structure, effectively making it empty and ready for fresh population. The function performs a clean slate operation by:

1. Acquiring exclusive access to the ProcArrayLock to ensure atomic updates
2. Resetting the count of known assigned transaction IDs to zero
3. Resetting both tail and head pointers to zero, indicating an empty array
4. Releasing the lock to allow other processes to access the structure

This operation is typically performed during recovery initialization or when transitioning between different recovery states. The function ensures thread-safe operation by acquiring an exclusive lock on the ProcArrayLock, preventing concurrent access during the reset operation.

The reset operation does not actually clear the array contents but rather resets the metadata that tracks valid entries, effectively making the array appear empty to all accessor functions.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [ProcArrayStruct](../P/ProcArrayStruct.md)
  - LWLockAcquire
  - LWLockRelease
  - ProcArrayLock
  - LW_EXCLUSIVE
- Called from (representative examples):
  - xc_slow_answer_inc
  - ProcArrayApplyRecoveryInfo

## Notes and Other Information
- This is a static function accessible only within procarray.c
- Requires exclusive locking on ProcArrayLock for safe concurrent access
- Does not actually zero out array contents, only resets the metadata tracking valid entries
- Used during recovery operations when the known assigned transaction set needs to be completely refreshed
- Part of PostgreSQL's Hot Standby recovery mechanism
- The function is atomic - either all metadata is reset or none is (due to exclusive locking)
- Critical for maintaining consistency when transitioning between recovery states or restarting recovery processes