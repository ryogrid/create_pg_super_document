# SetupLockInTable

## Location
src/backend/storage/lmgr/lock.c: 1183 - 1363

## Overview
SetupLockInTable creates or locates shared LOCK and PROCLOCK objects in PostgreSQL's lock table, establishing the necessary data structures for tracking lock ownership and requests.

## Definition


## Detailed Description
SetupLockInTable is a critical internal function that manages the shared memory structures necessary for lock tracking. It operates in two phases: first finding or creating a LOCK object that represents the lockable resource, then finding or creating a PROCLOCK object that represents the association between a process and that lock.

When creating new LOCK objects, it initializes all the necessary fields including grant/wait masks, request counters, and linked lists for tracking processes. For PROCLOCK objects, it establishes the connection between the process and lock, sets up group leadership relationships for parallel query support, and maintains proper linkage in both the lock's process list and the process's lock list.

The function includes sophisticated memory management, automatically cleaning up unused LOCK objects when PROCLOCK creation fails, preventing memory leaks in shared memory. It also includes optional deadlock risk detection code that can warn about potentially dangerous lock acquisition patterns.

## Parameters / Member Variables
- : Pointer to the lock method configuration defining lock behavior and conflict rules
- : Pointer to PGPROC structure representing the process acquiring the lock
- : Pointer to LOCKTAG structure uniquely identifying the lockable object
- : Pre-computed hash value for the lock tag for efficient hash table operations
- : The specific lock mode being requested for validation and tracking

## Dependencies
- Functions called/Symbols referenced:
  - [hash_search_with_hash_value](../h/hash_search_with_hash_value.md) (for finding/creating shared hash table entries)
  - [ProcLockHashCode](../P/ProcLockHashCode.md) (for computing proclock hash codes)
  - LockHashPartition (for determining lock partition)
  - [dlist_init](../d/dlist_init.md), dlist_push_tail (for managing linked lists)
  - [dclist_init](../d/dclist_init.md) (for wait queue initialization)
  - MemSet (for memory initialization)
- Data structures used:
  - LOCK, PROCLOCK, PROCLOCKTAG (core lock structures)
  - LockMethodLockHash, LockMethodProcLockHash (shared hash tables)
- Called from (representative examples):
  - [LockAcquireExtended](../L/LockAcquireExtended.md) (main lock acquisition path)
  - [FastPathTransferRelationLocks](../F/FastPathTransferRelationLocks.md) (fast-path to shared table transfer)
  - VirtualXactLock (virtual transaction locking)

## Notes and Other Information
- Returns NULL on memory allocation failure, allowing callers to handle resource exhaustion gracefully
- Automatically garbage collects unused LOCK objects to prevent shared memory leaks
- Maintains bidirectional linkage between locks and processes for efficient traversal
- Includes compile-time deadlock risk detection code for debugging lock hierarchy violations
- Handles lock group leadership for parallel query coordination
- Critical for ensuring proper lock accounting and preventing race conditions in concurrent environments
- Must be called with appropriate partition lock held to ensure thread safety