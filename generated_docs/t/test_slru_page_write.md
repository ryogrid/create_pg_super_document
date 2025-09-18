# test_slru_page_write

## Location
[src/test/modules/test_slru/test_slru.c:57-84](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_slru/test_slru.c#L57-L84)

## Overview
A PostgreSQL function that writes data to a specific SLRU page for testing purposes, handling page initialization, data copying, and persistence to disk.

## Definition


## Detailed Description
This function provides a testing interface for writing data to SLRU (Simple Log-structured Record Update) pages. It takes a page number and data string, allocates and zeros a page in the SLRU cache, copies the provided data to the page buffer, marks the page as dirty and valid, then writes it to disk. The function handles proper locking to ensure thread-safe access to the SLRU control structure and maintains data integrity during the write operation.

## Parameters / Member Variables
-  (PG_GETARG_INT64(0)): The page number in the SLRU to write data to
-  (PG_GETARG_TEXT_PP(1)): Text data to be written to the page, converted to a C string

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64 (argument extraction)
  - text_to_cstring (text conversion)
  - [SimpleLruGetBankLock](../S/SimpleLruGetBankLock.md) (lock acquisition for the page)
  - LWLockAcquire/LWLockRelease (locking primitives)
  - [SimpleLruZeroPage](../S/SimpleLruZeroPage.md) (page allocation and initialization)
  - [SimpleLruWritePage](../S/SimpleLruWritePage.md) (page persistence to disk)
  - TestSlruCtl (global SLRU control structure)
  - SLRU_PAGE_VALID (page status constant)
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- This is a PostgreSQL C function that can be called from SQL
- Uses exclusive locking to prevent concurrent access to the same page
- Data is limited to BLCKSZ - 1 bytes to fit within a single page
- The function marks pages as dirty to ensure they are written to disk
- Includes assertions to verify internal consistency of page number mapping
- Part of the SLRU testing infrastructure for validating write operations