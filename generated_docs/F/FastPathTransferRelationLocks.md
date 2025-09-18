# FastPathTransferRelationLocks

## Location
src/backend/storage/lmgr/lock.c: 2712 - 2799

## Overview
FastPathTransferRelationLocks transfers locks for a specific relation from all backends' per-backend fast-path arrays to the shared hash table, typically when fast-path optimization becomes inefficient due to lock conflicts.

## Definition
```c
static bool FastPathTransferRelationLocks(LockMethod lockMethodTable, const LOCKTAG *locktag, uint32 hashcode)
```

## Detailed Description
This function implements a critical transition mechanism in PostgreSQL's locking system. When the fast-path locking mechanism becomes ineffective (usually due to lock conflicts), this function migrates all existing fast-path locks for a specific relation to the standard shared lock table. It iterates through all active backends (ProcGlobal->allProcs), examines their fast-path slots, and transfers any matching relation locks to the shared hash table using the standard locking infrastructure.

The function ensures proper concurrency control by acquiring both the partition lock for the shared hash table and individual backend fast-path info locks. It also performs database-level filtering to avoid unnecessary processing of backends operating on different databases. After successfully transferring each lock, it clears the corresponding fast-path slot to prevent duplicate lock tracking.

## Parameters / Member Variables
- `lockMethodTable`: The lock method configuration defining lock conflict rules and behavior
- `locktag`: Lock tag structure containing the relation OID and database information for the target relation
- `hashcode`: Pre-computed hash value for the lock tag to determine the appropriate partition

## Dependencies
- Functions called/Symbols referenced:
  - LockHashPartitionLock: Determines the appropriate partition lock for the hash code
  - LWLockAcquire/LWLockRelease: Low-level locking primitives for concurrency control
  - FAST_PATH_GET_BITS: Macro to check if a fast-path slot is in use
  - FAST_PATH_CHECK_LOCKMODE: Macro to verify specific lock modes in fast-path slots
  - FAST_PATH_CLEAR_LOCKMODE: Macro to clear specific lock modes from fast-path slots
  - SetupLockInTable: Creates or finds lock objects in the shared hash table
  - GrantLock: Grants the transferred lock in the shared lock table
  - FAST_PATH_LOCKNUMBER_OFFSET and FAST_PATH_BITS_PER_SLOT: Constants defining fast-path slot structure
- Called from (representative examples):
  - LockAcquireExtended: When fast-path optimization needs to be disabled
  - ConflictsWithRelationFastPath: When checking for potential fast-path conflicts

## Notes and Other Information
- Returns true on successful transfer, false if shared memory allocation fails
- Processes all backends in ProcGlobal->allProcs, excluding prepared transactions
- Uses database ID filtering to optimize processing by skipping irrelevant backends
- Acquires multiple locks in a specific order to prevent deadlocks: backend fpInfoLock first, then partition lock
- Clears fast-path slots after successful transfer to maintain consistency
- Critical for maintaining lock semantics when transitioning from optimized to standard locking
- Memory fencing considerations are noted in comments regarding database ID checks