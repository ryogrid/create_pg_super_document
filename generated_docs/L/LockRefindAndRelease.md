# LockRefindAndRelease

## Location
[src/backend/storage/lmgr/lock.c:3112-3215](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L3112-L3215)

## Overview
LockRefindAndRelease finds a lock in the shared lock table and releases it, ensuring proper cleanup and wakeup of waiting processes.

## Definition


## Detailed Description
This function is responsible for finding and releasing a lock that exists in the shared lock table. It performs a complete lock release operation including:

1. Re-finding the LOCK object in the shared hash table using the provided LOCKTAG
2. Locating the corresponding PROCLOCK object that associates the process with the lock
3. Verifying that the process actually holds the specified lock mode
4. Releasing the lock via UnGrantLock and performing cleanup with CleanUpLock
5. Optionally decrementing the strong lock count for relation locks when used in 2PC contexts

The function is used in two main scenarios:
- Releasing locks held by prepared transactions during commit (2PC)
- Releasing fast-path locks that were transferred to the main hash table

This is a low-level function that assumes the caller has verified the operation is safe to perform.

## Parameters / Member Variables
- : The lock method table containing lock configuration and mode information
- : The PGPROC structure representing the process that holds the lock
- : The LOCKTAG identifying the specific lock object to release
- : The specific lock mode to release (e.g., AccessShareLock, ExclusiveLock)
- : Whether to decrement the fast-path strong lock count (used for 2PC)

## Dependencies
- Functions called/Symbols referenced:
  - LockTagHashCode
  - LockHashPartitionLock
  - [hash_search_with_hash_value](../h/hash_search_with_hash_value.md)
  - [ProcLockHashCode](../P/ProcLockHashCode.md)
  - LOCKBIT_ON
  - [UnGrantLock](../U/UnGrantLock.md)
  - [CleanUpLock](../C/CleanUpLock.md)
  - ConflictsWithRelationFastPath
  - FastPathStrongLockHashPartition
- Called from (representative examples):
  - [LockReleaseAll](LockReleaseAll.md)
  - [lock_twophase_postcommit](../l/lock_twophase_postcommit.md)
  - VirtualXactLockTableCleanup

## Notes and Other Information
- This is a static function only used within the lock manager
- The function will PANIC if it cannot find the expected LOCK or PROCLOCK objects
- Proper partition locking (LWLock) is used to ensure thread safety
- The strong lock count decrementing is specifically needed for two-phase commit scenarios
- The caller must ensure there are no remaining LOCALLOCK objects pointing to the lock being released