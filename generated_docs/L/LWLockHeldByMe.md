# LWLockHeldByMe

## Location
[src/backend/storage/lmgr/lwlock.c:1895-1912](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lwlock.c#L1895-L1912)

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
  - [SlruSelectLRUPage](../S/SlruSelectLRUPage.md): SLRU buffer management
  - [TwoPhaseGetGXact](../T/TwoPhaseGetGXact.md): Two-phase commit processing
  - [dshash_dump](../d/dshash_dump.md): Dynamic shared hash table operations
  - [VacuumUpdateCosts](../V/VacuumUpdateCosts.md): Autovacuum cost management
  - [CheckpointerSlotMapping](../C/CheckpointerSlotMapping.md): Checkpointer slot operations
  - [ProcArrayEndTransaction](../P/ProcArrayEndTransaction.md): Process array transaction management
  - Various buffer management functions: UnpinBufferNoOwner, FlushOneBuffer, MarkBufferDirtyHint

## Notes and Other Information
- This function is explicitly documented as "debug support only" in the source code
- Performs a linear search through the held locks array, so performance scales with the number of held locks
- Returns true if the lock is found in any mode (shared or exclusive)
- Commonly used in assertions and debugging code throughout PostgreSQL's codebase
- The function provides no information about the lock mode, only ownership
- Essential for debugging lock-related issues and ensuring proper lock management
- Located in src/backend/storage/lmgr/lwlock.c:1895-1912