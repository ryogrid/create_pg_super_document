# SerialAdd

## Location
[src/backend/storage/lmgr/predicate.c:858-948](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L858-L948)

## Overview
Records a committed read-write serializable transaction ID and its minimum conflict commit sequence number in the pg_serial SLRU system.

## Definition
```c
static void SerialAdd(TransactionId xid, SerCommitSeqNo minConflictCommitSeqNo)
```

## Detailed Description
This function is responsible for persistently storing information about committed serializable transactions that had read-write conflicts. It manages the pg_serial SLRU (Simple Least Recently Used) buffer system to track transaction commit sequence numbers for serializable isolation level conflict detection.

The function performs several key operations:
1. **Validation**: Checks if the transaction ID is still relevant (not older than the global xmin/tailXid)
2. **SLRU Management**: Determines the target page and manages SLRU buffer allocation
3. **Page Initialization**: Zeros out new pages entering the active range from tailXid to headXid
4. **Concurrent Control**: Uses both SerialControlLock and SLRU bank locks for thread safety
5. **Data Storage**: Records the minimum conflict commit sequence number for the transaction

The function handles two main scenarios:
- **Cold Start**: When the SLRU is currently unused, it zeros out the entire active region
- **Normal Operation**: Only zeros out new pages that enter the tailXid-headXid range

## Parameters / Member Variables
- `xid`: The transaction ID of the committed read-write serializable transaction
- `minConflictCommitSeqNo`: The minimum commit sequence number of any transactions to which this transaction had a read-write conflict out (InvalidSerCommitSeqNo if no conflicts)

## Dependencies
- Functions called/Symbols referenced:
  - `TransactionIdIsValid`
  - `[TransactionIdPrecedes](../T/TransactionIdPrecedes.md)`
  - `[TransactionIdFollows](../T/TransactionIdFollows.md)`
  - `SerialPage`
  - `SerialNextPage`
  - `[SerialPagePrecedesLogically](SerialPagePrecedesLogically.md)`
  - `SerialValue`
  - `[SimpleLruGetBankLock](SimpleLruGetBankLock.md)`
  - `[SimpleLruZeroPage](SimpleLruZeroPage.md)`
  - `[SimpleLruReadPage](SimpleLruReadPage.md)`
  - `[LWLockAcquire](../L/LWLockAcquire.md)`/`LWLockRelease`
- Called from (representative examples):
  - `[SummarizeOldestCommittedSxact](SummarizeOldestCommittedSxact.md)`

## Notes and Other Information
- Critical for maintaining serializable isolation level guarantees in PostgreSQL
- Uses sophisticated locking protocol: acquires SerialControlLock first, then individual SLRU bank locks
- Optimizes by early return if the transaction is older than the global xmin (no longer needed)
- Handles SLRU page initialization efficiently by zeroing ranges of pages when needed
- The function can involve "trading locks" when initializing multiple intervening pages
- Marks SLRU pages as dirty after writing to ensure proper persistence
- An invalid minConflictCommitSeqNo indicates the transaction had no read-write conflicts out
- Essential component of PostgreSQL's Serializable Snapshot Isolation (SSI) implementation

## Simplified Source

```c
static void
SerialAdd(TransactionId xid, SerCommitSeqNo minConflictCommitSeqNo)
{
    // Validate transaction ID and get target page
    Assert(TransactionIdIsValid(xid));
    int64 targetPage = SerialPage(xid);
    LWLock *lock = SimpleLruGetBankLock(SerialSlruCtl, targetPage);

    LWLockAcquire(SerialControlLock, LW_EXCLUSIVE);

    // Check if xid is still relevant (not older than tailXid)
    TransactionId tailXid = serialControl->tailXid;
    if (!TransactionIdIsValid(tailXid) || TransactionIdPrecedes(xid, tailXid)) {
        LWLockRelease(SerialControlLock);
        return;  // Too old, no longer needed
    }

    // Determine if we need to initialize new pages
    bool isNewPage;
    int64 firstZeroPage;
    if (serialControl->headPage < 0) {
        // SLRU currently unused - zero entire range
        firstZeroPage = SerialPage(tailXid);
        isNewPage = true;
    } else {
        firstZeroPage = SerialNextPage(serialControl->headPage);
        isNewPage = SerialPagePrecedesLogically(serialControl->headPage, targetPage);
    }

    // Update head tracking
    if (!TransactionIdIsValid(serialControl->headXid) ||
        TransactionIdFollows(xid, serialControl->headXid))
        serialControl->headXid = xid;
    if (isNewPage)
        serialControl->headPage = targetPage;

    // Initialize pages if needed
    int slotno;
    if (isNewPage) {
        // Zero intervening pages
        for (;;) {
            lock = SimpleLruGetBankLock(SerialSlruCtl, firstZeroPage);
            LWLockAcquire(lock, LW_EXCLUSIVE);
            slotno = SimpleLruZeroPage(SerialSlruCtl, firstZeroPage);
            if (firstZeroPage == targetPage)
                break;
            firstZeroPage = SerialNextPage(firstZeroPage);
            LWLockRelease(lock);
        }
    } else {
        LWLockAcquire(lock, LW_EXCLUSIVE);
        slotno = SimpleLruReadPage(SerialSlruCtl, targetPage, true, xid);
    }

    // Store the conflict commit sequence number
    SerialValue(slotno, xid) = minConflictCommitSeqNo;
    SerialSlruCtl->shared->page_dirty[slotno] = true;

    LWLockRelease(lock);
    LWLockRelease(SerialControlLock);
}
```