# XactLockForVirtualXact

## Location
src/backend/storage/lmgr/lock.c: 4509 - 4559

## Overview
A static helper function that waits for completion of prepared transactions associated with a virtual transaction ID, handling both specific transaction IDs and discovery of multiple prepared transactions.

## Definition
```c
static bool XactLockForVirtualXact(VirtualTransactionId vxid, TransactionId xid, bool wait)
```

## Detailed Description
This function provides the core logic for waiting on transactions that may have been prepared with a specific virtual transaction ID. It operates in two modes: if a specific transaction ID is provided, it waits for that transaction to complete (similar to XactLockTableWait but with different assumptions). If no transaction ID is provided, it uses TwoPhaseGetXidByVirtualXID() to discover all prepared transactions that were known by the given virtual transaction ID before their PREPARE TRANSACTION command.

The function acquires and immediately releases a ShareLock on each transaction's lock tag, which effectively waits for the transaction to commit or abort. It handles multiple prepared transactions by iterating through all discovered XIDs when the 'more' flag is set by TwoPhaseGetXidByVirtualXID().

The function includes an optimization: if max_prepared_xacts is 0 (two-phase commit disabled), it returns immediately since there can be no prepared transactions to wait for.

## Parameters / Member Variables
- `vxid`: The virtual transaction ID to look up prepared transactions for
- `xid`: Specific transaction ID to wait for, or InvalidTransactionId to discover all prepared XIDs for the vxid
- `wait`: If true, blocks until transactions complete; if false, returns immediately if transactions are still active

## Dependencies
- Functions called/Symbols referenced:
  - TwoPhaseGetXidByVirtualXID
  - SET_LOCKTAG_TRANSACTION
  - LockAcquire
  - LockRelease
  - VirtualTransactionId (type)
  - LockAcquireResult (type)
  - LOCKTAG (type)
  - ShareLock
  - LOCKACQUIRE_NOT_AVAIL
- Called from (representative examples):
  - VirtualXactLock (multiple call sites)

## Notes and Other Information
- Static function, only used internally within lock.c
- Assumes provided xid is never a subtransaction and is prepared, committed, or aborted
- Uses ShareLock acquisition as a wait mechanism for transaction completion
- Handles the case where multiple prepared transactions exist for the same virtual transaction ID
- Short-circuits when two-phase commit is disabled (max_prepared_xacts == 0)
- Part of PostgreSQL's two-phase commit and virtual transaction locking infrastructure
- Critical for ensuring proper waiting behavior when virtual transactions have been prepared