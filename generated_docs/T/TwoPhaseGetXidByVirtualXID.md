# TwoPhaseGetXidByVirtualXID

## Location
[src/backend/access/transam/twophase.c:852-902](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L852-L902)

## Overview
TwoPhaseGetXidByVirtualXID looks up a prepared transaction's XID by searching for a matching virtual transaction ID (VXID) among transactions prepared since the last startup.

## Definition

```c
TransactionId
TwoPhaseGetXidByVirtualXID(VirtualTransactionId vxid,
						   bool *have_more)
```
## Detailed Description
TwoPhaseGetXidByVirtualXID searches through prepared transactions to find one that matches the given virtual transaction ID. The function only finds transactions prepared since the last database startup (not recovered transactions from previous sessions). If multiple matches are found, it returns any one of them and sets the have_more flag to indicate additional matches exist. Multiple matches would require a single process number to consume 2^32 local XIDs without an intervening database restart, which is extremely unlikely in practice.

## Parameters / Member Variables
- : The VirtualTransactionId to search for among prepared transactions
- : Output parameter set to true if multiple matching transactions are found

## Dependencies
- Functions called/Symbols referenced:
  - VirtualTransactionIdIsValid (to validate input VXID)
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease (for shared lock on TwoPhaseStateLock)
  - GetPGProcByNumber (to get process information)
  - GET_VXID_FROM_PGPROC (macro to extract VXID from process)
  - VirtualTransactionIdEquals (to compare VXIDs)
- Data structures accessed:
  - TwoPhaseState (global two-phase commit state)
  - [GlobalTransaction](../G/GlobalTransaction.md) (transaction structure)
  - [PGPROC](../P/PGPROC.md) (process information)
  - [VirtualTransactionId](../V/VirtualTransactionId.md) (virtual transaction ID structure)
- Called from:
  - [XactLockForVirtualXact](../X/XactLockForVirtualXact.md) (in lock manager)

## Notes and Other Information
- Only finds prepared transactions created since the last startup, not recovered ones
- Returns InvalidTransactionId if no matching VXID is found
- Uses shared locking to minimize contention while searching
- Skips invalid transactions (gxact->valid check)
- Asserts that matching transactions are not from redo operations (!gxact->inredo)
- Multiple matches are theoretically possible but extremely rare in practice
- Part of PostgreSQL's transaction locking infrastructure for virtual transaction IDs

## Simplified Source

```c
TransactionId
TwoPhaseGetXidByVirtualXID(VirtualTransactionId vxid, bool *have_more)
{
    int i;
    TransactionId result = InvalidTransactionId;

    Assert(VirtualTransactionIdIsValid(vxid));
    LWLockAcquire(TwoPhaseStateLock, LW_SHARED);

    // Search through all prepared transactions
    for (i = 0; i < TwoPhaseState->numPrepXacts; i++) {
        GlobalTransaction gxact = TwoPhaseState->prepXacts[i];
        PGPROC *proc;
        VirtualTransactionId proc_vxid;

        // Skip invalid transactions
        if (!gxact->valid)
            continue;

        // Get process info and extract its VXID
        proc = GetPGProcByNumber(gxact->pgprocno);
        GET_VXID_FROM_PGPROC(proc_vxid, *proc);

        // Check if this VXID matches what we're looking for
        if (VirtualTransactionIdEquals(vxid, proc_vxid)) {
            Assert(!gxact->inredo);  // Shouldn't be from redo

            // If we already found one, mark multiple matches
            if (result != InvalidTransactionId) {
                *have_more = true;
                break;
            }
            result = gxact->xid;
        }
    }

    LWLockRelease(TwoPhaseStateLock);
    return result;
}
```