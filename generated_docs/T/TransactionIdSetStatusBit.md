# TransactionIdSetStatusBit

## Location
[src/backend/access/transam/clog.c:661-734](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/clog.c#L661-L734)

## Overview
A low-level function that sets the commit status of a single transaction directly in the CLOG buffer page, handling bit manipulation and LSN updates.

## Definition

```c
static void
TransactionIdSetStatusBit(TransactionId xid, XidStatus status, XLogRecPtr lsn, int slotno)
```
## Detailed Description
TransactionIdSetStatusBit is the core bit-manipulation function for updating transaction statuses in the Commit Log (CLOG). It operates directly on the CLOG buffer pages, performing the low-level work of setting the 2-bit status field for a specific transaction ID.

The function calculates the exact byte and bit position within the CLOG page where the transaction's status is stored, then updates those bits to reflect the new status. Each transaction uses 2 bits (CLOG_BITS_PER_XACT) to encode its status, allowing for four possible states.

The function includes important safeguards for recovery scenarios, where some transactions might already be correctly marked during replay. It also maintains group LSNs, which are used for efficient flushing of CLOG pages to disk by tracking the highest LSN for groups of transactions on each page.

## Parameters
- : The transaction ID whose status is being updated
- : The new transaction status to set (XidStatus enum value)
- : The WAL Log Sequence Number associated with this status change
- : The SLRU slot number containing the CLOG page for this transaction

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdToByte, TransactionIdToBIndex, TransactionIdToPage
  - [LWLockHeldByMeInMode](../L/LWLockHeldByMeInMode.md), SimpleLruGetBankLock
  - XLogRecPtrIsInvalid, GetLSNIndex
  - CLOG_BITS_PER_XACT, CLOG_XACT_BITMASK
  - TRANSACTION_STATUS_* constants
- Called from:
  - [TransactionIdSetPageStatusInternal](TransactionIdSetPageStatusInternal.md) (multiple times)

## Notes and Other Information
- Requires caller to hold the corresponding SLRU bank lock in exclusive mode
- Includes assertion checks to verify the correct page is loaded and proper locking
- Handles recovery scenarios where transactions may already be in the target state
- Updates group LSN tracking for efficient page flushing
- Uses bit manipulation to pack multiple transaction statuses into single bytes
- During recovery, LSN updates are skipped since the LSN will be invalid

## Simplified Source

```c
static void TransactionIdSetStatusBit(TransactionId xid, XidStatus status,
                                      XLogRecPtr lsn, int slotno) {
    // Calculate byte and bit position within the CLOG page
    int byteno = TransactionIdToByte(xid);
    int bshift = TransactionIdToBIndex(xid) * CLOG_BITS_PER_XACT;
    char *byteptr = XactCtl->shared->page_buffer[slotno] + byteno;
    char curval = (*byteptr >> bshift) & CLOG_XACT_BITMASK;

    // Handle recovery case where transaction may already be committed
    if (InRecovery && status == TRANSACTION_STATUS_SUB_COMMITTED &&
        curval == TRANSACTION_STATUS_COMMITTED)
        return;

    // Verify valid state transition
    Assert(curval == 0 ||
           (curval == TRANSACTION_STATUS_SUB_COMMITTED &&
            status != TRANSACTION_STATUS_IN_PROGRESS) ||
           curval == status);

    // Update the transaction status bits
    char byteval = *byteptr;
    byteval &= ~(((1 << CLOG_BITS_PER_XACT) - 1) << bshift);  // Clear old status
    byteval |= (status << bshift);                             // Set new status
    *byteptr = byteval;

    // Update group LSN for efficient page flushing
    if (!XLogRecPtrIsInvalid(lsn)) {
        int lsnindex = GetLSNIndex(slotno, xid);
        if (XactCtl->shared->group_lsn[lsnindex] < lsn)
            XactCtl->shared->group_lsn[lsnindex] = lsn;
    }
}
```