# LWLockHeldByMe

## Location
src/backend/storage/lmgr/lwlock.c: 1895 - 1912

## Overview
A debugging function that checks whether the current process holds a specific LWLock in any mode.

## Definition
```c
bool LWLockHeldByMe(LWLock *lock)
```

## Detailed Description
LWLockHeldByMe is a utility function designed primarily for debugging and assertion purposes. It searches through the current process's list of held LWLocks to determine if a specific lock is currently held by the calling process. The function checks for lock ownership regardless of the lock mode (exclusive or shared).

The function iterates through the held_lwlocks array, which maintains a record of all LWLocks currently held by the process, and performs a simple pointer comparison to find a match. This makes it an efficient way to verify lock ownership during development and debugging scenarios.

## Parameters / Member Variables
- `lock`: Pointer to the LWLock to check for ownership

## Dependencies
- Global variables used:
  - num_held_lwlocks: Current count of held locks
  - held_lwlocks: Array containing information about currently held locks
- Returns: boolean value indicating whether the lock is held by the current process
- Called from (representative examples):
  - SlruSelectLRUPage: SLRU buffer management
  - TwoPhaseGetGXact: Two-phase commit processing
  - dshash_dump: Dynamic shared hash table operations
  - VacuumUpdateCosts: Autovacuum cost management
  - CheckpointerSlotMapping: Checkpointer slot operations
  - ProcArrayEndTransaction: Process array transaction management
  - Various buffer management functions: UnpinBufferNoOwner, FlushOneBuffer, MarkBufferDirtyHint

## Notes and Other Information
- This function is explicitly documented as "debug support only" in the source code
- Performs a linear search through the held locks array, so performance scales with the number of held locks
- Returns true if the lock is found in any mode (shared or exclusive)
- Commonly used in assertions and debugging code throughout PostgreSQL's codebase
- The function provides no information about the lock mode, only ownership
- Essential for debugging lock-related issues and ensuring proper lock management
- Located in src/backend/storage/lmgr/lwlock.c:1895-1912