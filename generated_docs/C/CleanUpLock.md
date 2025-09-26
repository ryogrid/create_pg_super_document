# CleanUpLock

## Location
[src/backend/storage/lmgr/lock.c:1638-1691](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L1638-L1691)

## Overview
CleanUpLock performs cleanup operations after a lock is released, including garbage collection of unused proclock and lock objects and awakening waiting processes when appropriate.

## Definition
```c
static void CleanUpLock(LOCK *lock, PROCLOCK *proclock,
                       LockMethod lockMethodTable, uint32 hashcode,
                       bool wakeupNeeded)
```

## Detailed Description
This static function handles the cleanup phase that follows lock release operations. It performs two main types of cleanup based on the current state:

1. **Proclock Cleanup**: If the releasing process no longer holds any locks on this object (holdMask == 0), it removes the PROCLOCK entry:
   - Removes the proclock from both the lock's procLocks list and the process's procLink list
   - Calculates the proclock hash code and removes it from the LockMethodProcLockHash table
   - This prevents accumulation of unused proclock entries

2. **Lock Object Cleanup**: If no processes are requesting this lock anymore (nRequested == 0), it removes the entire LOCK object:
   - Verifies that the procLocks list is empty (assertion)
   - Removes the lock from the LockMethodLockHash table
   - This garbage collection prevents memory leaks from unused lock objects

3. **Wakeup Processing**: If there are remaining waiters and wakeup is needed, calls ProcLockWakeup to awaken waiting processes

The function assumes the appropriate partition lock is held and maintains that state.

## Parameters / Member Variables
- `lock`: Pointer to the LOCK structure representing the locked resource
- `proclock`: Pointer to the PROCLOCK structure representing the process-lock relationship
- `lockMethodTable`: Lock method table needed for wakeup operations
- `hashcode`: Hash code for the lock object, used for efficient hash table operations
- `wakeupNeeded`: Boolean indicating whether waiting processes should be awakened

## Dependencies
- Functions called/Symbols referenced:
  - PROCLOCK_PRINT (debugging macro)
  - [dlist_delete](../d/dlist_delete.md) (list manipulation)
  - [ProcLockHashCode](../P/ProcLockHashCode.md) (hash calculation)
  - [hash_search_with_hash_value](../h/hash_search_with_hash_value.md) (hash table operations)
  - LOCK_PRINT (debugging macro)
  - [dlist_is_empty](../d/dlist_is_empty.md) (list checking)
  - [ProcLockWakeup](../P/ProcLockWakeup.md) (process awakening)
  - HASH_REMOVE (hash operation constant)
- Called from (representative examples):
  - [LockRelease](../L/LockRelease.md)
  - [LockReleaseAll](../L/LockReleaseAll.md)
  - [LockRefindAndRelease](../L/LockRefindAndRelease.md)
  - [RemoveFromWaitQueue](../R/RemoveFromWaitQueue.md)

## Notes and Other Information
- This is a static function internal to lock.c, designed to work in conjunction with UnGrantLock
- The function includes panic conditions if hash table corruption is detected, indicating serious system issues
- Garbage collection is automatic and helps prevent memory bloat in long-running systems
- The wakeupNeeded parameter is typically the return value from a prior UnGrantLock call
- Proper cleanup is essential for lock table integrity and performance
- The function assumes appropriate locking context (partition lock held) for safe hash table manipulation