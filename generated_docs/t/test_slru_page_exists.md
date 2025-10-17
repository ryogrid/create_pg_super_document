# test_slru_page_exists

## Location
[src/test/modules/test_slru/test_slru.c:130-143](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_slru/test_slru.c#L130-L143)

## Overview
A PostgreSQL test function that checks if a specific SLRU (Simple LRU) page physically exists on disk.

## Definition

```c
Datum
test_slru_page_exists(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is part of PostgreSQL's test_slru module, designed for testing SLRU functionality. It takes a page number as input and determines whether the corresponding SLRU page physically exists in storage. The function uses proper locking mechanisms to ensure thread-safe access to the SLRU control structure and returns a boolean result indicating page existence.

The function acquires an exclusive lock on the appropriate bank before checking page existence, ensuring data consistency during the check operation.

## Parameters / Member Variables
-  (int64): The SLRU page number to check for existence

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts the int64 argument from function call
  - : Gets the appropriate lock for the SLRU bank
  - : Acquires exclusive lock on the SLRU bank
  - : Checks if the physical page exists
  - : Releases the acquired lock
  - : Returns boolean result to SQL
- Called from (representative examples):
  - No direct callers found (likely called via SQL function interface)

## Notes and Other Information
- Located in src/test/modules/test_slru/test_slru.c:130-143
- Part of the test_slru extension for testing SLRU operations
- Uses TestSlruCtl as the SLRU control structure
- Properly handles locking to ensure thread safety
- Returns SQL boolean type for integration with PostgreSQL's function call interface

## Simplified Source

```c
Datum test_slru_page_exists(PG_FUNCTION_ARGS) {
    int64 pageno = PG_GETARG_INT64(0);
    bool found;

    // Get the appropriate lock for this page's bank
    LWLock *lock = SimpleLruGetBankLock(TestSlruCtl, pageno);

    // Check if page exists with proper locking
    LWLockAcquire(lock, LW_EXCLUSIVE);
    found = SimpleLruDoesPhysicalPageExist(TestSlruCtl, pageno);
    LWLockRelease(lock);

    PG_RETURN_BOOL(found);
}
```