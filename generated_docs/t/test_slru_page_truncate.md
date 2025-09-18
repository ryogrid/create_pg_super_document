# test_slru_page_truncate

## Location
[src/test/modules/test_slru/test_slru.c:176-184](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_slru/test_slru.c#L176-L184)

## Overview
A PostgreSQL test function that truncates the SLRU to the specified page number, removing all pages beyond that point.

## Definition
```c
Datum test_slru_page_truncate(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is part of PostgreSQL's test_slru module for testing SLRU truncation operations. It takes a page number as input and calls SimpleLruTruncate to remove all SLRU pages with numbers greater than or equal to the specified page number. This is a bulk operation that can remove multiple segments and is typically used for cleanup or to reset the SLRU to an earlier state.

The truncation operation is atomic and ensures that the SLRU remains in a consistent state after the operation completes.

## Parameters / Member Variables
- `pageno` (int64): The page number at which to truncate; all pages >= this number will be removed

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INT64`: Extracts the int64 argument from function call
  - [SimpleLruTruncate](../S/SimpleLruTruncate.md): Performs the truncation operation on the SLRU
  - `TestSlruCtl`: The test SLRU control structure
  - `PG_RETURN_VOID`: Returns void to SQL
- Called from (representative examples):
  - No direct callers found (likely called via SQL function interface)

## Notes and Other Information
- Located in src/test/modules/test_slru/test_slru.c:176-184
- Part of the test_slru extension for testing SLRU operations
- Performs bulk removal of pages beyond the specified truncation point
- Operation is atomic and maintains SLRU consistency
- Returns void as the operation is performed for its side effects
- Use with caution as it permanently removes data beyond the truncation point
- More efficient than deleting individual pages when removing large ranges