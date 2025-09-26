# VirtualXactLockTableInsert

## Location
[src/backend/storage/lmgr/lock.c:4437-4459](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L4437-L4459)

## Overview
Acquires a virtual transaction ID lock using PostgreSQL's fast-path locking mechanism to prevent conflicts with the current transaction's virtual transaction ID.

## Definition
```c
void VirtualXactLockTableInsert(VirtualTransactionId vxid)
```

## Detailed Description
This function implements fast-path virtual transaction locking by setting flags in the current process's PGPROC structure. It is called early in transaction startup, before the virtual transaction ID is advertised in the ProcArray. The fast-path approach avoids the overhead of creating full lock table entries since virtual transaction locks are simple and have predictable usage patterns.

The function uses two separate fields (fpLocalTransactionId and vxid.lxid) that typically contain the same value but serve different purposes: vxid.lxid is used by procarray.c for transaction visibility checks, while fpLocalTransactionId is protected by fpInfoLock and used exclusively by the locking subsystem to prevent race conditions.

Since virtual transaction locks are only released at transaction end, this function doesn't create entries in the local lock table - cleanup is handled by VirtualXactLockTableCleanup() called from LockReleaseAll().

## Parameters / Member Variables
- `vxid`: The virtual transaction ID to acquire a lock for, must be valid and match the current process's virtual transaction ID

## Dependencies
- Functions called/Symbols referenced:
  - VirtualTransactionIdIsValid
  - [VirtualTransactionId](VirtualTransactionId.md) (type)
  - InvalidLocalTransactionId
- Called from (representative examples):
  - [StartTransaction](../S/StartTransaction.md)
  - [InitRecoveryTransactionEnvironment](../I/InitRecoveryTransactionEnvironment.md)
  - LockHashPartitionLockByProc

## Notes and Other Information
- Uses fast-path locking to avoid lock table overhead for virtual transaction locks
- The function assumes no pre-existing lockers since the vxid hasn't been advertised yet
- Protected by fpInfoLock to ensure atomic updates to fast-path locking state
- Designed to work with the virtual transaction ID visibility system
- Part of PostgreSQL's transaction isolation and conflict detection mechanism
- Only sets fpVXIDLock flag and fpLocalTransactionId - actual cleanup handled elsewhere