# test_slru_page_readonly

## Location
[src/test/modules/test_slru/test_slru.c:111-129](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_slru/test_slru.c#L111-L129)

## Overview
A PostgreSQL function that reads data from a specific SLRU page in read-only mode, ensuring no write permissions are granted to the page during the operation.

## Definition

```c
Datum
test_slru_page_readonly(PG_FUNCTION_ARGS)
```
## Detailed Description
This function provides a testing interface for reading SLRU (Simple Log-structured Record Update) pages in read-only mode. Unlike the regular read function, this variant uses  which specifically avoids granting write permissions on the page. The function acquires the appropriate bank lock, reads the page from disk if necessary, verifies that the lock is held, retrieves the data from the page buffer, and returns it as a PostgreSQL text value. This read-only approach is useful for testing scenarios where you want to ensure no accidental modifications occur during page access.

## Parameters / Member Variables
-  (PG_GETARG_INT64(0)): The page number in the SLRU to read data from in read-only mode

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64 (argument extraction macro)
  - [SimpleLruGetBankLock](../S/SimpleLruGetBankLock.md) (lock acquisition for the page bank)
  - [SimpleLruReadPage_ReadOnly](../S/SimpleLruReadPage_ReadOnly.md) (read-only page reading function)
  - LWLockHeldByMe (lock verification assertion)
  - LWLockRelease (lock release)
  - TestSlruCtl (global SLRU control structure)
  - InvalidTransactionId (constant for transaction ID parameter)
  - cstring_to_text (text conversion for PostgreSQL return)
  - PG_RETURN_TEXT_P (PostgreSQL return macro)
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- This is a PostgreSQL C function that can be called from SQL
- Uses read-only page access to prevent any write operations on the page
- Includes an assertion to verify the lock is properly held by the current process
- The function automatically handles lock acquisition and release through SimpleLruReadPage_ReadOnly
- InvalidTransactionId is used as the transaction ID parameter
- Returns the raw page buffer contents as a text string
- Part of the SLRU testing infrastructure specifically for read-only access patterns
- Useful for testing scenarios where write protection is required during page access