# StartupSUBTRANS

## Location
[src/backend/access/transam/subtrans.c:309-354](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/subtrans.c#L309-L354)

## Overview
Initializes the SUBTRANS (subtransaction status) system during PostgreSQL startup, zeroing out the currently-active pages to ensure a clean slate after crash recovery.

## Definition

```c
void
StartupSUBTRANS(TransactionId oldestActiveXID)
```
## Detailed Description
StartupSUBTRANS is called once during postmaster or standalone-backend startup, after StartupXLOG has initialized the next transaction ID. It initializes the SUBTRANS system by zeroing out all pages that might contain active subtransaction status information.

The function determines the range of SUBTRANS pages that need to be initialized based on the oldest active transaction ID and the next transaction ID. It then iterates through all pages in this range, acquiring appropriate locks and zeroing each page using ZeroSUBTRANSPage.

Since PostgreSQL doesn't expect pg_subtrans to be valid across crashes, this initialization ensures that all currently-relevant pages start with a clean state. Future page extensions through ExtendSUBTRANS will similarly zero new pages without regard to previous disk contents.

## Parameters / Member Variables
- `oldestActiveXID`: The oldest transaction ID of any prepared transaction, or nextXid if there are no prepared transactions
## Dependencies
- Functions called/Symbols referenced:
  - [TransactionIdToPage](../T/TransactionIdToPage.md)
  - XidFromFullTransactionId
  - [SimpleLruGetBankLock](SimpleLruGetBankLock.md)
  - [ZeroSUBTRANSPage](../Z/ZeroSUBTRANSPage.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease
- Called from (representative examples):
  - [StartupXLOG](StartupXLOG.md) (during crash recovery and normal startup)

## Notes and Other Information
- Must be called exactly once during startup after TransamVariables->nextXid is initialized
- Uses bank locking to efficiently handle page initialization across multiple pages
- Handles wraparound cases where page numbers exceed MaxTransactionId
- Critical for ensuring subtransaction status consistency after server restarts

## Simplified Source

```c
// Simplified version of StartupSUBTRANS
void StartupSUBTRANS(TransactionId oldestActiveXID) {
    FullTransactionId nextXid;
    int64 startPage, endPage;
    LWLock *prevlock = NULL;
    LWLock *lock;

    // Calculate the range of SUBTRANS pages that need initialization
    startPage = TransactionIdToPage(oldestActiveXID);
    nextXid = TransamVariables->nextXid;
    endPage = TransactionIdToPage(XidFromFullTransactionId(nextXid));

    // Zero out all pages from startPage to endPage
    for (;;) {
        // Acquire appropriate bank lock for this page
        lock = SimpleLruGetBankLock(SubTransCtl, startPage);
        if (prevlock != lock) {
            if (prevlock)
                LWLockRelease(prevlock);
            LWLockAcquire(lock, LW_EXCLUSIVE);
            prevlock = lock;
        }

        // Zero the current page
        ZeroSUBTRANSPage(startPage);

        // Check if we've processed all pages
        if (startPage == endPage)
            break;

        // Move to next page, handling wraparound
        startPage++;
        if (startPage > TransactionIdToPage(MaxTransactionId))
            startPage = 0;
    }

    LWLockRelease(lock);
}
```

Key simplifications made:
- Preserved the core page initialization loop and locking logic
- Maintained wraparound handling for transaction ID page numbers
- Kept essential bank locking optimization for performance
- Simplified comments to focus on high-level operations
- Preserved all critical function calls and control flow