# test_slru_scan_cb

## Location
[src/test/modules/test_slru/test_slru.c:50-56](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_slru/test_slru.c#L50-L56)

## Overview
A callback function used by the SLRU test module to scan and delete all files in an SLRU directory during testing operations.

## Definition


## Detailed Description
This function serves as a callback for SLRU directory scanning operations in the test module. It provides a simple wrapper around the  function, adding logging functionality to track when the callback is invoked. The function is designed to delete all files encountered during the scan operation, making it useful for cleanup operations in SLRU testing scenarios.

## Parameters / Member Variables
- : SLRU control structure containing metadata and configuration for the SLRU being scanned
- : Name of the current file being processed during the directory scan
- : Segment page number associated with the current file
- : Generic data pointer passed through from the calling context (unused in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - elog (for logging NOTICE messages)
  - [SlruScanDirCbDeleteAll](../S/SlruScanDirCbDeleteAll.md) (the actual deletion callback function)
- Called from (representative examples):
  - [test_slru_delete_all](test_slru_delete_all.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the test_slru.c file
- The function always returns the result of , which is typically a boolean indicating success
- The logging message helps track callback invocation during testing scenarios
- Part of the SLRU (Simple Log-structured Record Update) testing infrastructure