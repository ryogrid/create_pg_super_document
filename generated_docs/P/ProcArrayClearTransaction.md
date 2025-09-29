# ProcArrayClearTransaction

## Location
[src/backend/storage/ipc/procarray.c:907-966](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L907-L966)

## Overview
ProcArrayClearTransaction clears transaction fields from a PGPROC entry after successfully preparing a 2-phase transaction, while maintaining the transaction's visibility through the associated global transaction entry.

## Definition

```c
void
ProcArrayClearTransaction(PGPROC *proc)
```
## Detailed Description
This function is specifically designed for 2-phase commit (2PC) transactions. After a transaction is successfully prepared, this function clears the transaction fields from the process's PGPROC entry without actually removing the transaction from the running transaction list. The transaction remains visible as running through its associated global transaction (gxact) entry in the ProcArray.

The function clears the XID, virtual XID, xmin, and subtransaction information from the PGPROC, and increments the transaction completion count to ensure proper snapshot behavior. This prevents snapshot reuse issues that could occur if the prepared transaction wasn't properly accounted for.

## Parameters / Member Variables
- : Pointer to the PGPROC structure whose transaction fields need to be cleared

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - InvalidTransactionId
  - InvalidLocalTransactionId
  - PROC_VACUUM_STATE_MASK
- Called from (representative examples):
  - [PrepareTransaction](PrepareTransaction.md)

## Notes and Other Information
- Exclusively used in 2-phase commit scenarios after successful transaction preparation
- Requires exclusive ProcArrayLock to maintain consistency of xactCompletionCount
- Does not actually remove the transaction from the running set - the gxact entry keeps it visible
- Clears both main transaction and subtransaction information
- Increments xactCompletionCount to prevent problematic snapshot reuse
- Could potentially be optimized with atomic variables, but 2PC overhead makes this unnecessary
- May be combined with subsequent ProcArrayRemove() in future optimizations

## Simplified Source

```c
void ProcArrayClearTransaction(PGPROC *proc)
{
    int pgxactoff;

    // Need exclusive lock to update completion count and maintain consistency
    LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

    pgxactoff = proc->pgxactoff;

    // Clear transaction identifiers from both global and process arrays
    ProcGlobal->xids[pgxactoff] = InvalidTransactionId;
    proc->xid = InvalidTransactionId;

    // Clear virtual transaction ID and related fields
    proc->vxid.lxid = InvalidLocalTransactionId;
    proc->xmin = InvalidTransactionId;
    proc->recoveryConflictPending = false;

    // Verify process state is clean
    Assert(!(proc->statusFlags & PROC_VACUUM_STATE_MASK));
    Assert(!proc->delayChkptFlags);

    // Increment completion count to prevent problematic snapshot reuse
    // (GetSnapshotData omits current transaction, so this prevents
    // reusing stale snapshots that might not count prepared transaction)
    TransamVariables->xactCompletionCount++;

    // Clear subtransaction cache if present
    Assert(ProcGlobal->subxidStates[pgxactoff].count == proc->subxidStatus.count &&
           ProcGlobal->subxidStates[pgxactoff].overflowed == proc->subxidStatus.overflowed);

    if (proc->subxidStatus.count > 0 || proc->subxidStatus.overflowed)
    {
        ProcGlobal->subxidStates[pgxactoff].count = 0;
        ProcGlobal->subxidStates[pgxactoff].overflowed = false;
        proc->subxidStatus.count = 0;
        proc->subxidStatus.overflowed = false;
    }

    LWLockRelease(ProcArrayLock);
}
```