# SubTransPagePrecedes

## Location
[src/backend/access/transam/subtrans.c:435-447](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/subtrans.c#L435-L447)

## Overview
Determines whether one SUBTRANS page logically precedes another for truncation purposes, handling PostgreSQL's modular transaction ID arithmetic correctly.

## Definition

```c
static bool
SubTransPagePrecedes(int64 page1, int64 page2)
```
## Detailed Description
SubTransPagePrecedes is a comparison function used by the SimpleLru system to determine the relative age of SUBTRANS pages for truncation operations. It's analogous to CLOGPagePrecedes() and handles the complexities of PostgreSQL's modular transaction ID arithmetic.

The function converts page numbers to representative transaction IDs by multiplying by SUBTRANS_XACTS_PER_PAGE and adding FirstNormalTransactionId + 1. It then uses TransactionIdPrecedes to determine if page1 precedes page2, checking both the start of page1 against the start of page2, and the start of page1 against the end of page2.

This dual check ensures that page1 completely precedes all transactions that could be stored in page2, which is essential for safe truncation operations.

## Parameters / Member Variables
- : First SUBTRANS page number to compare
- : Second SUBTRANS page number to compare

## Dependencies
- Functions called/Symbols referenced:
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md) (used twice for comprehensive comparison)
  - SUBTRANS_XACTS_PER_PAGE
  - FirstNormalTransactionId
- Called from (representative examples):
  - SubTransCtl initialization (as page comparison function)
  - [SUBTRANSShmemInit](SUBTRANSShmemInit.md) (during shared memory setup)

## Notes and Other Information
- Static function used internally by SUBTRANS system
- Analogous to CLOGPagePrecedes() for commit log pages
- Essential for correct SimpleLru truncation behavior
- Handles wraparound cases through TransactionIdPrecedes
- Double-checks both start and end boundaries of pages
- Critical for maintaining data consistency during SUBTRANS cleanup operations
- Used as callback function in SimpleLru control structure

## Simplified Source

```c
// Simplified version of SubTransPagePrecedes
static bool SubTransPagePrecedes(int64 page1, int64 page2) {
    // Core logic step 1: Convert page numbers to representative transaction IDs
    TransactionId xid1 = ((TransactionId) page1) * SUBTRANS_XACTS_PER_PAGE + FirstNormalTransactionId + 1;
    TransactionId xid2 = ((TransactionId) page2) * SUBTRANS_XACTS_PER_PAGE + FirstNormalTransactionId + 1;

    // Core logic step 2: Check if page1 completely precedes page2
    // Must check both start of page1 vs start of page2, and start of page1 vs end of page2
    return (TransactionIdPrecedes(xid1, xid2) &&
            TransactionIdPrecedes(xid1, xid2 + SUBTRANS_XACTS_PER_PAGE - 1));
}
```

Key simplifications made:
- Consolidated variable declarations with initialization
- Added clear explanation of the dual comparison logic
- Focused on the core page precedence algorithm
- Simplified comments to explain the why behind the logic