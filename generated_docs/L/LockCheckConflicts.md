# LockCheckConflicts

## Location
[src/backend/storage/lmgr/lock.c:1429-1557](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L1429-L1557)

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
  - [ProcSleep](../P/ProcSleep.md)
  - [ProcLockWakeup](../P/ProcLockWakeup.md)

## Notes and Other Information
- Returns true if there is a conflict, false if the lock can be granted
- The algorithm is O(N) in the number of processes holding locks on the object when lock groups are involved
- [Relation](../R/Relation.md) extension locks have special conflict semantics even within lock groups to prevent concurrent relation growth issues
- Uses efficient bitwise operations for initial conflict detection before falling back to detailed analysis
- Includes extensive debugging output via PROCLOCK_PRINT macros
- The complexity of this function reflects PostgreSQL's sophisticated approach to concurrent access control

## Simplified Source

```c
// Simplified version of LockCheckConflicts
bool LockCheckConflicts(LockMethod lockMethodTable,
                       LOCKMODE lockmode,
                       LOCK *lock,
                       PROCLOCK *proclock) {
    int numLockModes = lockMethodTable->numLockModes;
    int conflictMask = lockMethodTable->conflictTab[lockmode];
    LOCKMASK myLocks = proclock->holdMask;

    // Step 1: Quick global conflict check
    // If no locks conflict with our request, we're done
    if (!(conflictMask & lock->grantMask)) {
        return false;  // No conflict
    }

    // Step 2: Calculate conflicts after removing our own locks
    int conflictsRemaining[MAX_LOCKMODES];
    int totalConflictsRemaining = 0;

    for (int i = 1; i <= numLockModes; i++) {
        if ((conflictMask & LOCKBIT_ON(i)) == 0) {
            conflictsRemaining[i] = 0;
            continue;
        }
        // Count granted locks of this mode
        conflictsRemaining[i] = lock->granted[i];
        // Subtract our own locks (they don't conflict with us)
        if (myLocks & LOCKBIT_ON(i)) {
            conflictsRemaining[i]--;
        }
        totalConflictsRemaining += conflictsRemaining[i];
    }

    // Step 3: If no conflicts after removing our locks, we're good
    if (totalConflictsRemaining == 0) {
        return false;  // No conflict
    }

    // Step 4: Handle lock groups - group members usually don't conflict
    // Exception: relation extension locks always conflict
    if (LOCK_LOCKTAG(*lock) == LOCKTAG_RELATION_EXTEND) {
        return true;  // Always conflicts for relation extension
    }

    // If not using lock groups, it's a definite conflict
    if (proclock->groupLeader == MyProc && MyProc->lockGroupLeader == NULL) {
        return true;  // Conflict - no group sharing
    }

    // Step 5: Subtract locks held by our lock group members
    dlist_iter proclock_iter;
    dlist_foreach(proclock_iter, &lock->procLocks) {
        PROCLOCK *otherproclock = dlist_container(PROCLOCK, lockLink, proclock_iter.cur);

        // Check if this is a different process in our same lock group
        if (proclock != otherproclock &&
            proclock->groupLeader == otherproclock->groupLeader &&
            (otherproclock->holdMask & conflictMask) != 0) {

            // Subtract group member's conflicting locks
            int intersectMask = otherproclock->holdMask & conflictMask;
            for (int i = 1; i <= numLockModes; i++) {
                if ((intersectMask & LOCKBIT_ON(i)) != 0) {
                    conflictsRemaining[i]--;
                    totalConflictsRemaining--;
                }
            }

            // If all conflicts resolved by group sharing, no conflict
            if (totalConflictsRemaining == 0) {
                return false;
            }
        }
    }

    // Step 6: Real conflict remains after all exclusions
    return true;
}
```

Key simplifications made:
- Removed debug PROCLOCK_PRINT statements for clarity
- Simplified variable declarations and moved them closer to use
- Added step-by-step comments explaining the algorithm flow
- Removed detailed error checking (PANIC conditions) to focus on main logic
- Consolidated the lock group iteration logic into clearer sections
- Used more descriptive comments to explain complex bitwise operations
- Maintained all essential algorithm steps and correctness