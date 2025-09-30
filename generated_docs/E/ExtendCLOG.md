# ExtendCLOG

## Location
[src/backend/access/transam/clog.c:959-999](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/clog.c#L959-L999)

## Overview
ExtendCLOG ensures that the CLOG has sufficient space for newly allocated transaction IDs by creating and zeroing new CLOG pages when needed.

## Definition
```c
void ExtendCLOG(TransactionId newestXact)
```

## Detailed Description
ExtendCLOG is responsible for extending the CLOG (Commit Log) to accommodate newly allocated transaction IDs. The function is called while holding XidGenLock and is optimized to be very fast in the common case. It only performs work when the newest transaction ID is the first XID of a new page, with a special case for handling wraparound where the first XID of page zero is FirstNormalTransactionId.

When extension is needed, the function creates a new zeroed CLOG page and generates an XLOG entry to ensure the operation is properly logged for crash recovery. The function uses exclusive locking on the appropriate CLOG page bank to ensure thread safety during page creation.

## Parameters / Member Variables
- `newestXact`: The newest transaction ID that requires CLOG space

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdToPgIndex
  - TransactionIdEquals
  - [TransactionIdToPage](../T/TransactionIdToPage.md)
  - [SimpleLruGetBankLock](../S/SimpleLruGetBankLock.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [ZeroCLOGPage](../Z/ZeroCLOGPage.md)
  - [LWLockRelease](../L/LWLockRelease.md)
- Global variables accessed:
  - FirstNormalTransactionId
  - XactCtl
- Called from:
  - [GetNewTransactionId](../G/GetNewTransactionId.md) (src/backend/access/transam/varsup.c:204)

## Notes and Other Information
- Called while holding XidGenLock, so must be very fast in the common case
- Only extends CLOG at the first XID of a new page to minimize overhead
- Handles wraparound case where first XID of page zero is FirstNormalTransactionId
- No actual I/O typically occurs unless dirty pages need to be written to make room
- Uses ZeroCLOGPage with WAL logging enabled to ensure crash recovery consistency
- Uses exclusive locking to prevent concurrent access during page creation

## Simplified Source

```c
void
ExtendCLOG(TransactionId newestXact)
{
    // Only extend at first XID of a new page (optimization)
    // Special case: after wraparound, first XID of page zero is FirstNormalTransactionId
    if (TransactionIdToPgIndex(newestXact) != 0 &&
        !TransactionIdEquals(newestXact, FirstNormalTransactionId)) {
        return; // No work needed
    }

    // Calculate which CLOG page we need and get its lock
    int64 pageno = TransactionIdToPage(newestXact);
    LWLock *lock = SimpleLruGetBankLock(XactCtl, pageno);

    // Create and zero the new CLOG page with WAL logging
    LWLockAcquire(lock, LW_EXCLUSIVE);
    ZeroCLOGPage(pageno, true); // true = make XLOG entry
    LWLockRelease(lock);
}
```