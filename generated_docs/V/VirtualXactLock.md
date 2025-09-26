# VirtualXactLock

## Location
src/backend/storage/lmgr/lock.c: 4560 - 4670

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
  - XactLockForVirtualXact
  - SET_LOCKTAG_VIRTUALTRANSACTION
  - ProcNumberGetProc
  - LockTagHashCode
  - LockHashPartitionLock
  - SetupLockInTable
  - GrantLock
  - LockAcquire
  - LockRelease
  - VirtualTransactionId (type)
  - LOCKTAG (type)
  - PGPROC (type)
  - PROCLOCK (type)
  - LWLock (type)
  - DEFAULT_LOCKMETHOD
  - ExclusiveLock
  - ShareLock
- Called from (representative examples):
  - WaitForOlderSnapshots
  - ResolveRecoveryConflictWithVirtualXIDs
  - WaitForLockersMultiple
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