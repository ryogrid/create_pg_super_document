# test_slru_page_read

## Location
src/test/modules/test_slru/test_slru.c: 92 - 110

## Overview
A PostgreSQL function that reads data from a specific SLRU page, with optional write access control, returning the page contents as text.

## Definition


## Detailed Description
This function provides a testing interface for reading data from SLRU (Simple Log-structured Record Update) pages. It takes a page number and a write permission flag, acquires an exclusive lock on the appropriate page bank, reads the page from disk if necessary (loading it into the buffer cache), retrieves the data from the page buffer, and returns it as a PostgreSQL text value. The function handles proper locking to ensure thread-safe access and can control whether the page should be marked as available for writing.

## Parameters / Member Variables
-  (PG_GETARG_INT64(0)): The page number in the SLRU to read data from
-  (PG_GETARG_BOOL(1)): Boolean flag indicating whether the page should be marked as writable

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64/PG_GETARG_BOOL (argument extraction macros)
  - SimpleLruGetBankLock (lock acquisition for the page bank)
  - LWLockAcquire/LWLockRelease (locking primitives)
  - SimpleLruReadPage (page reading from disk/cache)
  - TestSlruCtl (global SLRU control structure)
  - InvalidTransactionId (constant for transaction ID parameter)
  - cstring_to_text (text conversion for PostgreSQL return)
  - PG_RETURN_TEXT_P (PostgreSQL return macro)
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- This is a PostgreSQL C function that can be called from SQL
- Uses exclusive locking to prevent concurrent modifications during read
- The write_ok parameter controls page access permissions after reading
- InvalidTransactionId is used as the transaction ID parameter to SimpleLruReadPage
- Returns the raw page buffer contents as a text string
- Part of the SLRU testing infrastructure for validating read operations
- The function will load pages from disk if they are not already in the buffer cache