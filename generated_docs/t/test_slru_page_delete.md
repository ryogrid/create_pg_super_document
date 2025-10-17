# test_slru_page_delete

## Location
[src/test/modules/test_slru/test_slru.c:161-175](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_slru/test_slru.c#L161-L175)

## Overview
A PostgreSQL test function that deletes an entire SLRU segment containing the specified page.

## Definition
```c
Datum test_slru_page_delete(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is part of PostgreSQL's test_slru module for testing SLRU deletion operations. It takes a page number as input, calculates which segment contains that page, and then calls SlruDeleteSegment to remove the entire segment from storage. The function provides logging to indicate which segment was deleted.

Like other SLRU operations, this function works at the segment level rather than individual page level, meaning all pages within the segment are deleted together.

## Parameters / Member Variables
- `pageno` (int64): The SLRU page number whose containing segment should be deleted

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INT64`: Extracts the int64 argument from function call
  - `[FileTag](../F/FileTag.md)`: Structure for identifying file segments
  - `SLRU_PAGES_PER_SEGMENT`: Constant defining pages per segment
  - [SlruDeleteSegment](../S/SlruDeleteSegment.md): Deletes the specified segment
  - `TestSlruCtl`: The test SLRU control structure
  - `elog`: Logs notice messages
  - `PG_RETURN_VOID`: Returns void to SQL
- Called from (representative examples):
  - No direct callers found (likely called via SQL function interface)

## Notes and Other Information
- Located in src/test/modules/test_slru/test_slru.c:161-175
- Part of the test_slru extension for testing SLRU operations
- Calculates segment number by dividing page number by SLRU_PAGES_PER_SEGMENT
- Deletes entire segments rather than individual pages
- Provides logging with NOTICE level messages to track operations
- Returns void as the operation is performed for its side effects
- Use with caution as it permanently removes data from storage

## Simplified Source

```c
Datum test_slru_page_delete(PG_FUNCTION_ARGS) {
    // Get page number from function arguments
    int64 pageno = PG_GETARG_INT64(0);

    // Calculate which segment contains this page
    FileTag ftag;
    ftag.segno = pageno / SLRU_PAGES_PER_SEGMENT;

    // Delete the entire segment
    SlruDeleteSegment(TestSlruCtl, ftag.segno);

    // Log the operation
    elog(NOTICE, "Called SlruDeleteSegment() for segment %lld",
         (long long) ftag.segno);

    PG_RETURN_VOID();
}
```