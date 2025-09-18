# LockCheckConflicts

## Location
src/backend/storage/lmgr/lock.c: 1429 - 1557

## Overview
LockCheckConflicts determines whether a requested lock conflicts with locks already granted, accounting for the fact that locks held by the same process or lock group do not conflict with each other.

## Definition
```c
bool LockCheckConflicts(LockMethod lockMethodTable,
                       LOCKMODE lockmode,
                       LOCK *lock,
                       PROCLOCK *proclock)
```

## Detailed Description
This function implements the core logic for PostgreSQL's lock conflict detection. It performs a sophisticated analysis that considers:

1. **Global Conflict Check**: First checks if the requested lock mode conflicts with any currently granted locks using bitwise operations on lock masks
2. **Self-Lock Exclusion**: Subtracts locks already held by the same process, as a process's own locks never conflict with each other
3. **Lock Group Handling**: For processes participating in lock groups (parallel query), excludes locks held by other members of the same lock group, since group members typically don't conflict with each other
4. **Special Cases**: Relation extension locks are treated specially - they conflict even within lock groups to prevent issues with concurrent relation extension

The function uses efficient bitmask operations and iterates through the list of processes holding locks on the object to determine conflicts after accounting for group membership.

## Parameters / Member Variables
- `lockMethodTable`: The lock method definition containing conflict rules and lock mode information
- `lockmode`: The lock mode being requested 
- `lock`: Pointer to the LOCK object representing the resource being locked
- `proclock`: Pointer to the PROCLOCK structure representing the requesting process's relationship to this lock

## Dependencies
- Functions called/Symbols referenced:
  - PROCLOCK_PRINT (debugging macro)
  - LOCKBIT_ON (macro for lock bit operations)
  - LOCK_LOCKTAG (macro to extract lock tag)
  - dlist_foreach (list iteration)
  - dlist_container (list container extraction)
- Called from (representative examples):
  - [LockAcquireExtended](LockAcquireExtended.md)
  - ProcSleep
  - ProcLockWakeup

## Notes and Other Information
- Returns true if there is a conflict, false if the lock can be granted
- The algorithm is O(N) in the number of processes holding locks on the object when lock groups are involved
- [Relation](../R/Relation.md) extension locks have special conflict semantics even within lock groups to prevent concurrent relation growth issues
- Uses efficient bitwise operations for initial conflict detection before falling back to detailed analysis
- Includes extensive debugging output via PROCLOCK_PRINT macros
- The complexity of this function reflects PostgreSQL's sophisticated approach to concurrent access control