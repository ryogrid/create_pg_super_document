# VirtualXactLockTableCleanup

## Location
[src/backend/storage/lmgr/lock.c:4460-4508](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L4460-L4508)

## Overview
Cleans up virtual transaction locks by clearing fast-path lock state and releasing any materialized locks from the main lock table.

## Definition
```c
void VirtualXactLockTableCleanup(void)
```

## Detailed Description
This function handles cleanup of virtual transaction locks at transaction end. It operates in two phases: first, it clears the fast-path lock state in the current process's PGPROC structure, then it checks if the lock was materialized (transferred to the main lock table) and releases it if necessary.

The function determines whether a lock was materialized by checking if fpVXIDLock was cleared while fpLocalTransactionId remained valid - this indicates that another process attempted to acquire a conflicting lock and caused the virtual transaction lock to be transferred from fast-path to the main lock table. If materialization occurred, the function constructs the appropriate lock tag and calls LockRefindAndRelease() to release the lock and wake any waiting processes.

This cleanup is essential for transaction isolation and preventing lock leaks, ensuring that virtual transaction locks are properly released regardless of whether they remained in fast-path or were materialized.

## Parameters / Member Variables
None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - LocalTransactionIdIsValid
  - SET_LOCKTAG_VIRTUALTRANSACTION
  - [LockRefindAndRelease](../L/LockRefindAndRelease.md)
  - LocalTransactionId (type)
  - [VirtualTransactionId](VirtualTransactionId.md) (type)
  - [LOCKTAG](../L/LOCKTAG.md) (type)
  - InvalidLocalTransactionId
  - INVALID_PROC_NUMBER
  - DEFAULT_LOCKMETHOD
  - ExclusiveLock
- Called from (representative examples):
  - [LockReleaseAll](../L/LockReleaseAll.md)
  - [ShutdownRecoveryTransactionEnvironment](../S/ShutdownRecoveryTransactionEnvironment.md)
  - LockHashPartitionLockByProc

## Notes and Other Information
- Called automatically by LockReleaseAll() during transaction cleanup
- Handles both fast-path and materialized virtual transaction locks
- The fpVXIDLock and fpLocalTransactionId state indicates whether materialization occurred
- Uses LockRefindAndRelease() rather than normal lock release to handle the materialized case
- Critical for preventing virtual transaction lock leaks and ensuring proper cleanup
- Works in conjunction with VirtualXactLockTableInsert() to manage virtual transaction lock lifecycle
- Protected by fpInfoLock to ensure atomic state changes