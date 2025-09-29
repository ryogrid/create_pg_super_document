# ProcArrayEndTransaction

## Location
[src/backend/storage/ipc/procarray.c:667-730](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L667-L730)

## Overview
Marks a transaction as no longer running in the shared process array, handling both commit and abort cases with optimizations for lock contention.

## Definition

```c
void
ProcArrayEndTransaction(PGPROC *proc, TransactionId latestXid)
```
## Detailed Description
ProcArrayEndTransaction handles the end of a transaction (either commit or abort) by clearing the transaction's visibility in the shared process array. The function implements several optimizations to minimize lock contention:

For transactions with valid XIDs, it uses two strategies:
1. **Fast path**: If ProcArrayLock can be acquired immediately, it directly calls ProcArrayEndTransactionInternal
2. **Group clearing**: If the lock is contended, it uses ProcArrayGroupClearXid to batch multiple transaction endings together

For transactions without XIDs (read-only transactions), no locking is required for the main XID clearing, but vacuum-related status flags still need synchronized updates.

The function ensures that transaction visibility changes are atomic with respect to snapshot taking, preventing race conditions where a transaction might appear to still be running during snapshot construction.

## Parameters / Member Variables
- : Pointer to the PGPROC structure of the ending transaction (typically MyProc)
- : Latest transaction ID among the main XID and subtransactions, or InvalidTransactionId if no XID

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdIsValid
  - [LWLockConditionalAcquire](../L/LWLockConditionalAcquire.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockHeldByMe](../L/LWLockHeldByMe.md)
  - [ProcArrayEndTransactionInternal](ProcArrayEndTransactionInternal.md)
  - [ProcArrayGroupClearXid](ProcArrayGroupClearXid.md)
  - InvalidLocalTransactionId
  - InvalidTransactionId
  - PROC_VACUUM_STATE_MASK
  - ProcGlobal

- Called from (representative examples):
  - [CommitTransaction](../C/CommitTransaction.md)
  - [AbortTransaction](../A/AbortTransaction.md)

## Notes and Other Information
- Uses conditional lock acquisition to optimize for the common case of low contention
- [Group](../G/Group.md) XID clearing mechanism reduces lock contention when multiple transactions end simultaneously
- Read-only transactions (no XID) require minimal synchronization
- Vacuum status flags require special handling even for read-only transactions
- The function assumes transaction commit/abort has already been written to WAL and pg_xact
- Caller must provide latestXid because PGPROC's subxid information might be incomplete
- Clears various transaction-related fields including vxid.lxid, xmin, and recovery conflict flags
- The optimization strategy balances between immediate processing and batched group processing

## Simplified Source

```c
// Simplified version of ProcArrayEndTransaction
void ProcArrayEndTransaction(PGPROC *proc, TransactionId latestXid) {
    if (TransactionIdIsValid(latestXid)) {
        // Transaction has XID - need synchronized clearing
        Assert(TransactionIdIsValid(proc->xid));

        // Try fast path: acquire lock immediately
        if (LWLockConditionalAcquire(ProcArrayLock, LW_EXCLUSIVE)) {
            // Clear XID directly
            ProcArrayEndTransactionInternal(proc, latestXid);
            LWLockRelease(ProcArrayLock);
        } else {
            // Use group clearing for better performance under contention
            ProcArrayGroupClearXid(proc, latestXid);
        }
    } else {
        // Read-only transaction - no XID to clear
        Assert(!TransactionIdIsValid(proc->xid));
        Assert(proc->subxidStatus.count == 0);

        // Clear transaction fields without locking
        proc->vxid.lxid = InvalidLocalTransactionId;
        proc->xmin = InvalidTransactionId;
        proc->delayChkptFlags = 0;
        proc->recoveryConflictPending = false;

        // Handle vacuum status flags (requires locking)
        if (proc->statusFlags & PROC_VACUUM_STATE_MASK) {
            LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
            proc->statusFlags &= ~PROC_VACUUM_STATE_MASK;
            ProcGlobal->statusFlags[proc->pgxactoff] = proc->statusFlags;
            LWLockRelease(ProcArrayLock);
        }
    }
}
```

Key simplifications made:
- Removed verbose comments and documentation within code
- Consolidated assertion checks for clarity
- Simplified conditional logic flow
- Added high-level descriptive comments for main branches
- Abstracted complex locking details into clear action descriptions
- Maintained essential algorithm structure and correctness