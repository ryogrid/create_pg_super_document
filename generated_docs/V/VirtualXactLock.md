# VirtualXactLock

## Location
[src/backend/storage/lmgr/lock.c:4560-4670](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L4560-L4670)

## Overview
Waits for a virtual transaction to complete, handling both active transactions and prepared transactions, with support for fast-path and materialized lock mechanisms.

## Definition
```c
bool VirtualXactLock(VirtualTransactionId vxid, bool wait)
```

## Detailed Description
This is the main public interface for waiting on virtual transactions in PostgreSQL. The function implements a sophisticated locking protocol that handles several complex cases:

1. **Recovered Prepared Transactions**: If the VXID represents a recovered prepared transaction, it delegates to XactLockForVirtualXact() to wait on the actual transaction ID.

2. **Active Transactions**: For normal active transactions, it first checks if the transaction is still running by examining the target process's PGPROC structure under fpInfoLock protection.

3. **Lock Materialization**: If waiting is required and the target process has a fast-path VXID lock, it converts (materializes) that lock into a full lock table entry to enable proper waiting behavior.

4. **Transaction ID Handling**: The function captures the process's current XID (if any) to optimize subsequent waits on prepared transactions.

The function provides both blocking (wait=true) and non-blocking (wait=false) modes. In non-blocking mode, it returns true if the transaction has completed, false if still active. In blocking mode, it waits until completion and returns true.

## Parameters / Member Variables
- `vxid`: The virtual transaction ID to wait for
- `wait`: If true, blocks until the transaction completes; if false, just checks current status

## Dependencies
- Functions called/Symbols referenced:
  - VirtualTransactionIdIsValid
  - VirtualTransactionIdIsRecoveredPreparedXact
  - [XactLockForVirtualXact](../X/XactLockForVirtualXact.md)
  - SET_LOCKTAG_VIRTUALTRANSACTION
  - [ProcNumberGetProc](../P/ProcNumberGetProc.md)
  - [LockTagHashCode](../L/LockTagHashCode.md)
  - LockHashPartitionLock
  - [SetupLockInTable](../S/SetupLockInTable.md)
  - [GrantLock](../G/GrantLock.md)
  - [LockAcquire](../L/LockAcquire.md)
  - [LockRelease](../L/LockRelease.md)
  - [VirtualTransactionId](VirtualTransactionId.md) (type)
  - [LOCKTAG](../L/LOCKTAG.md) (type)
  - [PGPROC](../P/PGPROC.md) (type)
  - [PROCLOCK](../P/PROCLOCK.md) (type)
  - [LWLock](../L/LWLock.md) (type)
  - DEFAULT_LOCKMETHOD
  - ExclusiveLock
  - ShareLock
- Called from (representative examples):
  - [WaitForOlderSnapshots](../W/WaitForOlderSnapshots.md)
  - [ResolveRecoveryConflictWithVirtualXIDs](../R/ResolveRecoveryConflictWithVirtualXIDs.md)
  - [WaitForLockersMultiple](../W/WaitForLockersMultiple.md)
  - LockHashPartitionLockByProc

## Notes and Other Information
- Central function for virtual transaction conflict resolution and waiting
- Handles race conditions between fast-path and materialized locks
- Used extensively in index creation, hot standby conflict resolution, and lock management
- The fpInfoLock acquisition ensures atomic examination of transaction state
- Materialization process converts fast-path locks to enable multi-process waiting
- Returns different semantics based on wait parameter: status check vs. blocking wait
- Critical for PostgreSQL's transaction isolation and concurrency control mechanisms
- Handles both normal and prepared transaction scenarios transparently

## Simplified Source

```c
bool
VirtualXactLock(VirtualTransactionId vxid, bool wait)
{
    LOCKTAG tag;
    PGPROC *proc;
    TransactionId xid = InvalidTransactionId;

    Assert(VirtualTransactionIdIsValid(vxid));

    // Handle recovered prepared transactions
    if (VirtualTransactionIdIsRecoveredPreparedXact(vxid))
        return XactLockForVirtualXact(vxid, vxid.localTransactionId, wait);

    SET_LOCKTAG_VIRTUALTRANSACTION(tag, vxid);

    // Get target process
    proc = ProcNumberGetProc(vxid.procNumber);
    if (proc == NULL)
        return XactLockForVirtualXact(vxid, InvalidTransactionId, wait);

    // Check transaction status under lock
    LWLockAcquire(&proc->fpInfoLock, LW_EXCLUSIVE);

    if (proc->vxid.procNumber != vxid.procNumber ||
        proc->fpLocalTransactionId != vxid.localTransactionId) {
        // VXID ended
        LWLockRelease(&proc->fpInfoLock);
        return XactLockForVirtualXact(vxid, InvalidTransactionId, wait);
    }

    // If not waiting, just return status
    if (!wait) {
        LWLockRelease(&proc->fpInfoLock);
        return false;
    }

    // Materialize fast-path lock if needed
    if (proc->fpVXIDLock) {
        // Set up lock table entry for waiting
        uint32 hashcode = LockTagHashCode(&tag);
        LWLock *partitionLock = LockHashPartitionLock(hashcode);

        LWLockAcquire(partitionLock, LW_EXCLUSIVE);

        PROCLOCK *proclock = SetupLockInTable(LockMethods[DEFAULT_LOCKMETHOD],
                                              proc, &tag, hashcode, ExclusiveLock);
        if (!proclock) {
            LWLockRelease(partitionLock);
            LWLockRelease(&proc->fpInfoLock);
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
                           errmsg("out of shared memory")));
        }

        GrantLock(proclock->tag.myLock, proclock, ExclusiveLock);
        LWLockRelease(partitionLock);
        proc->fpVXIDLock = false;
    }

    // Capture current XID for later use
    xid = proc->xid;
    LWLockRelease(&proc->fpInfoLock);

    // Wait for the virtual transaction
    LockAcquire(&tag, ShareLock, false, false);
    LockRelease(&tag, ShareLock, false);

    return XactLockForVirtualXact(vxid, xid, wait);
}
```