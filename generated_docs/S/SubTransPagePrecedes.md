# SubTransPagePrecedes

## Location
[src/backend/access/transam/subtrans.c:435-447](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/subtrans.c#L435-L447)

## Overview
Determines whether one SUBTRANS page logically precedes another for truncation purposes, handling PostgreSQL's modular transaction ID arithmetic correctly.

## Definition


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
  - SUBTRANSShmemInit (during shared memory setup)

## Notes and Other Information
- Static function used internally by SUBTRANS system
- Analogous to CLOGPagePrecedes() for commit log pages
- Essential for correct SimpleLru truncation behavior
- Handles wraparound cases through TransactionIdPrecedes
- Double-checks both start and end boundaries of pages
- Critical for maintaining data consistency during SUBTRANS cleanup operations
- Used as callback function in SimpleLru control structure