# StartupSUBTRANS

## Location
src/backend/access/transam/subtrans.c: 309 - 354

## Overview
Initializes the SUBTRANS (subtransaction status) system during PostgreSQL startup, zeroing out the currently-active pages to ensure a clean slate after crash recovery.

## Definition


## Detailed Description
StartupSUBTRANS is called once during postmaster or standalone-backend startup, after StartupXLOG has initialized the next transaction ID. It initializes the SUBTRANS system by zeroing out all pages that might contain active subtransaction status information.

The function determines the range of SUBTRANS pages that need to be initialized based on the oldest active transaction ID and the next transaction ID. It then iterates through all pages in this range, acquiring appropriate locks and zeroing each page using ZeroSUBTRANSPage.

Since PostgreSQL doesn't expect pg_subtrans to be valid across crashes, this initialization ensures that all currently-relevant pages start with a clean state. Future page extensions through ExtendSUBTRANS will similarly zero new pages without regard to previous disk contents.

## Parameters / Member Variables
- : The oldest transaction ID of any prepared transaction, or nextXid if there are no prepared transactions

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdToPage
  - XidFromFullTransactionId
  - SimpleLruGetBankLock
  - ZeroSUBTRANSPage
  - LWLockAcquire/LWLockRelease
- Called from (representative examples):
  - StartupXLOG (during crash recovery and normal startup)

## Notes and Other Information
- Must be called exactly once during startup after TransamVariables->nextXid is initialized
- Uses bank locking to efficiently handle page initialization across multiple pages
- Handles wraparound cases where page numbers exceed MaxTransactionId
- Critical for ensuring subtransaction status consistency after server restarts