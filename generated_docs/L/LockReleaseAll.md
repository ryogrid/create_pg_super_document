# LockReleaseAll

## Location
src/backend/storage/lmgr/lock.c: 2169 - 2443

## Overview
LockReleaseAll releases all locks of a specified lock method held by the current process, with options to release either all locks including session locks or only non-session locks.

## Definition


## Detailed Description
LockReleaseAll is a comprehensive lock cleanup function that releases multiple locks held by the current process for a specific lock method. The function operates in two main phases:

1. **Local Lock Table Scan**: Iterates through the process's local lock table (LOCALLOCK entries), handling:
   - Fast-path lock releases for relation locks
   - Session vs. transaction lock differentiation
   - Resource owner cleanup
   - Marking locks for release in the shared lock table

2. **Shared Lock Table Scan**: Processes each lock partition to:
   - Release locks marked in the releaseMask
   - Handle locks that may have been missed in the local table scan
   - Wake up waiting processes through CleanUpLock

Key features:
- Supports both complete cleanup (allLocks=true) and selective cleanup (allLocks=false)
- Handles PostgreSQL's fast-path optimization for relation locks
- Includes special handling for virtual transaction locks
- Performs extensive validation and debugging checks
- Manages resource owner relationships properly

## Parameters / Member Variables
- : Identifier of the lock method whose locks should be released (e.g., DEFAULT_LOCKMETHOD)
- : If true, release all locks including session locks; if false, release only non-session (transaction) locks

## Dependencies
- Functions called/Symbols referenced:
  - VirtualXactLockTableCleanup
  - hash_seq_init/hash_seq_search (hash table iteration)
  - RemoveLocalLock
  - LOCALLOCK_LOCKMETHOD/LOCALLOCK_LOCKTAG (macros)
  - ResourceOwnerForgetLock
  - EligibleForRelationFastPath
  - FastPathUnGrantRelationLock
  - LockRefindAndRelease
  - LockHashPartitionLockByIndex
  - UnGrantLock
  - CleanUpLock
  - LockTagHashCode
  - dlist operations (dlist_foreach_modify, dlist_container, etc.)
- Called from (representative examples):
  - DiscardAll
  - logicalrep_worker_onexit
  - ProcReleaseLocks
  - ShutdownPostgres
  - LockHashPartitionLockByProc

## Notes and Other Information
- Two-phase approach prevents dangling pointers between local and shared lock tables
- Special handling for VXID (Virtual Transaction ID) locks via VirtualXactLockTableCleanup
- Includes debugging warnings for tuple locks held at commit (should be short-duration)
- Fast-path locks require special handling and may need to be "refound" in shared table
- Uses partition-based locking to minimize contention during cleanup
- Extensive assertions verify lock state consistency throughout the process
- Located in src/backend/storage/lmgr/lock.c at lines 2169-2443
- Critical for transaction abort/commit cleanup and session termination
- Optimizes empty partition scanning to avoid unnecessary lock acquisition