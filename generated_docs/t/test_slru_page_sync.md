# test_slru_page_sync

## Location
src/test/modules/test_slru/test_slru.c: 144 - 160

## Overview
A PostgreSQL test function that synchronizes (flushes) an SLRU segment containing the specified page to disk.

## Definition
```c
Datum test_slru_page_sync(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is part of PostgreSQL's test_slru module for testing SLRU synchronization operations. It takes a page number as input, calculates which segment contains that page, and then calls SlruSyncFileTag to flush the entire segment to disk. The function provides detailed logging to show which segment was synchronized and the file path involved.

Note that this operation flushes the entire segment file that contains the specified page, not just the individual page itself, as SLRU operates on segment boundaries.

## Parameters / Member Variables
- `pageno` (int64): The SLRU page number whose containing segment should be synchronized

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INT64`: Extracts the int64 argument from function call
  - `FileTag`: Structure for identifying file segments
  - `SLRU_PAGES_PER_SEGMENT`: Constant defining pages per segment
  - `[SlruSyncFileTag](../S/SlruSyncFileTag.md)`: Synchronizes the segment file to disk
  - `TestSlruCtl`: The test SLRU control structure
  - `elog`: Logs notice messages
  - `PG_RETURN_VOID`: Returns void to SQL
- Called from (representative examples):
  - No direct callers found (likely called via SQL function interface)

## Notes and Other Information
- Located in src/test/modules/test_slru/test_slru.c:144-160
- Part of the test_slru extension for testing SLRU operations
- Calculates segment number by dividing page number by SLRU_PAGES_PER_SEGMENT
- Provides verbose logging with NOTICE level messages
- Synchronizes entire segments rather than individual pages
- Returns void as the operation is performed for its side effects