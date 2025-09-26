# LWLockHeldByMeInMode

## Location
[src/backend/storage/lmgr/lwlock.c:1939-1949](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lwlock.c#L1939-L1949)

## Overview
A debugging function that checks whether the current process holds a specific LWLock in a specific mode (shared or exclusive).

## Definition
```c
bool LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode)
```

## Detailed Description
LWLockHeldByMeInMode is a debugging utility that provides more precise lock ownership checking than LWLockHeldByMe. While LWLockHeldByMe only checks if a lock is held in any mode, this function verifies that the current process holds a specific lock in a specific mode (either LW_SHARED or LW_EXCLUSIVE).

The function searches through the held_lwlocks array maintained by the current process, checking both the lock pointer and the mode for each held lock. This level of granularity is important for debugging scenarios where the lock mode matters, such as ensuring that a process has exclusive access before performing write operations or verifying that shared access is sufficient for read operations.

This function is extensively used throughout PostgreSQL's codebase in assertions and debugging code, particularly in areas where lock mode semantics are critical for correctness.

## Parameters / Member Variables
- `lock`: Pointer to the LWLock to check for ownership
- `mode`: The specific LWLockMode to check for (LW_SHARED or LW_EXCLUSIVE)

## Dependencies
- Types referenced:
  - LWLock: The lock structure being checked
  - LWLockMode: Enumeration of lock modes (LW_SHARED, LW_EXCLUSIVE)
- Global variables used:
  - num_held_lwlocks: Current count of held locks by the process
  - held_lwlocks: Array containing lock and mode information for currently held locks
- Returns: boolean value indicating whether the lock is held in the specified mode
- Called from (representative examples):
  - TransactionIdSetPageStatusInternal: Transaction commit log operations
  - SimpleLruZeroPage, SimpleLruReadPage: SLRU (Simple LRU) buffer management
  - MarkAsPreparingGuts, RemoveGXact: Two-phase commit processing
  - dshash_delete_entry, dshash_seq_next: Dynamic shared hash table operations
  - BufferIsExclusiveLocked, MarkBufferDirty: Buffer management operations
  - ProcArrayEndTransactionInternal: Process array transaction management
  - write_relmap_file: Relation mapping file operations

## Notes and Other Information
- This function is explicitly documented as "debug support only" in the source code
- Provides mode-specific lock ownership verification, unlike the more general LWLockHeldByMe
- Essential for debugging scenarios where lock mode semantics are critical
- Used extensively in assertions throughout PostgreSQL's buffer management, transaction processing, and shared data structure code
- The function performs both pointer comparison for the lock and value comparison for the mode
- Particularly important in multi-mode locking scenarios where shared and exclusive access have different semantic meanings
- Located in src/backend/storage/lmgr/lwlock.c:1939-1949