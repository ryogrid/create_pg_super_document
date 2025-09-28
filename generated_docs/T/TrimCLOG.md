# TrimCLOG

## Location
[src/backend/access/transam/clog.c:892-936](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/clog.c#L892-L936)

## Overview
TrimCLOG zeroes out unused portions of the current CLOG page to ensure clean state after startup/recovery, preventing potential issues from inconsistent transaction status data.

## Definition
```c
void TrimCLOG(void)
```

## Detailed Description
TrimCLOG is called exactly once at the end of PostgreSQL startup or recovery to clean up the current CLOG (Commit Log) page. The function zeroes out unused positions in the current CLOG page to handle cases where WAL replay might have resulted in a nextXID value that is less than the last XID actually used in the previous database lifecycle. This situation can theoretically occur because subtransaction commits write to CLOG but don't generate WAL entries.

The function operates on the current CLOG page only, as future pages will be automatically zeroed when first used. If nextXID is exactly at a page boundary, no action is taken since the current page likely doesn't exist yet.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - XidFromFullTransactionId
  - [TransactionIdToPage](TransactionIdToPage.md)
  - [SimpleLruGetBankLock](../S/SimpleLruGetBankLock.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - TransactionIdToPgIndex
  - TransactionIdToByte
  - TransactionIdToBIndex
  - [SimpleLruReadPage](../S/SimpleLruReadPage.md)
  - MemSet
  - [LWLockRelease](../L/LWLockRelease.md)
- Global variables accessed:
  - TransamVariables->nextXid
  - XactCtl
  - CLOG_BITS_PER_XACT
- Called from:
  - [StartupXLOG](../S/StartupXLOG.md) (src/backend/access/transam/xlog.c:6077)

## Notes and Other Information
- Must be called exactly ONCE at the end of startup/recovery
- Uses exclusive locking on the appropriate CLOG page bank to ensure thread safety
- Only processes the current page if nextXID is not at a page boundary (TransactionIdToPgIndex(xid) != 0)
- Marks the affected CLOG page as dirty after modification to ensure proper persistence
- The zeroing operation preserves already-committed transaction bits while clearing unused positions

## Simplified Source

```c
// Simplified version of TrimCLOG
void TrimCLOG(void) {
    // Get the next transaction ID and determine which CLOG page it belongs to
    TransactionId xid = XidFromFullTransactionId(TransamVariables->nextXid);
    int64 pageno = TransactionIdToPage(xid);
    LWLock *lock = SimpleLruGetBankLock(XactCtl, pageno);

    // Acquire exclusive lock on the CLOG page
    LWLockAcquire(lock, LW_EXCLUSIVE);

    // Only process if nextXID is not at the start of a page
    if (TransactionIdToPgIndex(xid) != 0) {
        // Calculate position within the page
        int byteno = TransactionIdToByte(xid);
        int bshift = TransactionIdToBIndex(xid) * CLOG_BITS_PER_XACT;

        // Read the CLOG page into memory
        int slotno = SimpleLruReadPage(XactCtl, pageno, false, xid);
        char *byteptr = XactCtl->shared->page_buffer[slotno] + byteno;

        // Zero unused bits in current byte (preserve used bits)
        *byteptr &= (1 << bshift) - 1;

        // Zero all remaining bytes in the page
        MemSet(byteptr + 1, 0, BLCKSZ - byteno - 1);

        // Mark page as dirty so it gets written to disk
        XactCtl->shared->page_dirty[slotno] = true;
    }

    // Release the lock
    LWLockRelease(lock);
}
```

Key simplifications made:
- Removed detailed comments explaining the theoretical edge case rationale
- Simplified variable declarations and grouping
- Added high-level comments explaining each major step
- Condensed the bit manipulation logic with clearer variable names
- Focused on the main algorithm flow rather than implementation details
- Preserved all essential functionality and logic structure