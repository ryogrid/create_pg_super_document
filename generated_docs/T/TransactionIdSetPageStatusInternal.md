# TransactionIdSetPageStatusInternal

## Location
[src/backend/access/transam/clog.c:364-440](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/clog.c#L364-L440)

## Overview
The low-level function that actually records the final transaction state in the commit log page, handling the physical bit manipulation without any locking.

## Definition

```c
static void
TransactionIdSetPageStatusInternal(TransactionId xid, int nsubxids,
								   TransactionId *subxids, XidStatus status,
								   XLogRecPtr lsn, int64 pageno)
```
## Detailed Description
This is the core low-level function responsible for physically updating transaction status bits in the CLOG page. It operates under the assumption that the caller has already acquired appropriate locks, and it focuses solely on the mechanical aspects of updating the commit log data.

The function implements a careful ordering strategy when setting transaction status for commits:

1. **For Commits**: Sets subtransactions to SUB_COMMITTED status first, then sets the main transaction to COMMITTED status. This ordering ensures atomicity from the perspective of concurrent readers - they never see a state where the main transaction appears committed but subtransactions appear uncommitted.

2. **For Aborts**: Sets status directly without intermediate states since there's no SUB_ABORTED status.

The function also handles async commit semantics by ensuring proper coordination with ongoing page writes when an LSN is provided, preventing premature disk writes before WAL flushing is complete.

## Parameters / Member Variables
- `xid`: Main transaction ID to update (can be InvalidTransactionId when only updating subtransactions)
- `nsubxids`: Number of subtransaction IDs in the subxids array
- `*subxids`: Array of subtransaction IDs to update
- `status`: The XidStatus to set (COMMITTED, ABORTED, or SUB_COMMITTED)
- `lsn`: WAL log sequence number (used for async commit coordination)
- `pageno`: The CLOG page number being updated
## Dependencies
- Functions called/Symbols referenced:
  - [SimpleLruReadPage](../S/SimpleLruReadPage.md)
  - [TransactionIdSetStatusBit](TransactionIdSetStatusBit.md)
  - [TransactionIdToPage](TransactionIdToPage.md)
  - XLogRecPtrIsInvalid
  - [LWLockHeldByMeInMode](../L/LWLockHeldByMeInMode.md)
  - [SimpleLruGetBankLock](../S/SimpleLruGetBankLock.md)
  - XactCtl
  - TRANSACTION_STATUS_COMMITTED
  - TRANSACTION_STATUS_ABORTED  
  - TRANSACTION_STATUS_SUB_COMMITTED
- Called from (representative examples):
  - [TransactionIdSetPageStatus](TransactionIdSetPageStatus.md)
  - [TransactionGroupUpdateXidStatus](TransactionGroupUpdateXidStatus.md)

## Notes and Other Information
- This is a static internal function that assumes the caller has acquired appropriate locks
- Includes assertions to verify proper lock ownership and valid status values
- Implements careful ordering for commits to maintain atomicity from concurrent readers' perspective
- Handles async commit synchronization with ongoing page writes
- For commits, subtransactions are first marked SUB_COMMITTED, then the main transaction is marked COMMITTED, then subtransactions are updated to COMMITTED
- Marks the SLRU page as dirty after updates to ensure eventual disk persistence
- Contains assertions to verify all transactions are actually on the expected page

## Simplified Source

```c
static void
TransactionIdSetPageStatusInternal(TransactionId xid, int nsubxids,
                                   TransactionId *subxids, XidStatus status,
                                   XLogRecPtr lsn, int64 pageno)
{
    int slotno;
    int i;

    // Validate input parameters
    Assert(status == TRANSACTION_STATUS_COMMITTED ||
           status == TRANSACTION_STATUS_ABORTED ||
           (status == TRANSACTION_STATUS_SUB_COMMITTED && !TransactionIdIsValid(xid)));

    // Read the CLOG page into memory
    // Wait for writes if doing async commit (lsn is valid)
    slotno = SimpleLruReadPage(XactCtl, pageno, XLogRecPtrIsInvalid(lsn), xid);

    // Update main transaction if valid
    if (TransactionIdIsValid(xid)) {
        // For commits: mark subtransactions as SUB_COMMITTED first
        if (status == TRANSACTION_STATUS_COMMITTED) {
            for (i = 0; i < nsubxids; i++) {
                TransactionIdSetStatusBit(subxids[i],
                                        TRANSACTION_STATUS_SUB_COMMITTED,
                                        lsn, slotno);
            }
        }

        // Set the main transaction status
        TransactionIdSetStatusBit(xid, status, lsn, slotno);
    }

    // Set final status for all subtransactions
    for (i = 0; i < nsubxids; i++) {
        TransactionIdSetStatusBit(subxids[i], status, lsn, slotno);
    }

    // Mark page as dirty for eventual persistence
    XactCtl->shared->page_dirty[slotno] = true;
}
```