# test_slru_delete_all

## Location
[src/test/modules/test_slru/test_slru.c:185-197](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_slru/test_slru.c#L185-L197)

## Overview
A PostgreSQL test function that deletes all SLRU segments by scanning the entire SLRU directory and removing each segment found.

## Definition
```c
Datum test_slru_delete_all(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is part of PostgreSQL's test_slru module for testing complete SLRU cleanup operations. It uses SlruScanDirectory with a callback function (test_slru_scan_cb) to iterate through all SLRU segments in the directory and delete them. The callback function internally calls SlruScanDirCbDeleteAll to perform the actual deletion of each segment.

This is a comprehensive cleanup function that removes all SLRU data, typically used for testing scenarios where a clean slate is needed.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [SlruScanDirectory](../S/SlruScanDirectory.md): Scans the SLRU directory and calls callback for each segment
  - `TestSlruCtl`: The test SLRU control structure
  - [test_slru_scan_cb](test_slru_scan_cb.md): Callback function that handles deletion of each segment
  - `PG_RETURN_VOID`: Returns void to SQL
- Related callback function:
  - [test_slru_scan_cb](test_slru_scan_cb.md): Calls SlruScanDirCbDeleteAll to delete segments
- Called from (representative examples):
  - No direct callers found (likely called via SQL function interface)

## Notes and Other Information
- Located in src/test/modules/test_slru/test_slru.c:185-197
- Part of the test_slru extension for testing SLRU operations
- Performs complete cleanup of all SLRU segments
- Uses callback-based directory scanning for thorough deletion
- The callback function provides logging with NOTICE level messages
- Returns void as the operation is performed for its side effects
- Use with extreme caution as it removes all SLRU data permanently
- Ideal for test cleanup and resetting SLRU to empty state

## Simplified Source

```c
Datum test_slru_delete_all(PG_FUNCTION_ARGS) {
    // Scan the SLRU directory and delete all segments
    // The callback internally calls SlruScanDirCbDeleteAll() for deletion
    SlruScanDirectory(TestSlruCtl, test_slru_scan_cb, NULL);

    PG_RETURN_VOID();
}
```