# SimpleLruDoesPhysicalPageExist

## Location
src/backend/access/transam/slru.c: 743 - 800

## Overview
Determines whether a specific SLRU page exists on disk by checking file existence and size, used for validation before attempting read operations.

## Definition


## Detailed Description
SimpleLruDoesPhysicalPageExist performs a physical disk check to determine if a specific SLRU page exists and is accessible. The function implements a comprehensive validation process that goes beyond simple file existence checking.

The function operates by:
1. **Segment Calculation**: Converts the logical page number into a segment number and relative page offset within that segment
2. **File Access**: Attempts to open the corresponding SLRU segment file in read-only mode
3. **Size Validation**: Uses lseek to determine the file size and verify it contains enough data for the requested page
4. **Error Handling**: Distinguishes between expected conditions (file not found) and actual errors (I/O failures)
5. **Statistics Tracking**: Updates SLRU page existence check statistics for monitoring purposes

This function is particularly important for systems that need to verify page availability before attempting read operations, helping to avoid unnecessary I/O errors and providing better error handling in recovery scenarios.

## Parameters / Member Variables
- : SlruCtl control structure containing SLRU configuration and shared state information
- : 64-bit logical page number to check for existence on disk

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_count_slru_page_exists
  - SlruFileName
  - OpenTransientFile
  - lseek
  - CloseTransientFile
  - SlruReportIOError
  - SLRU_PAGES_PER_SEGMENT
  - PG_BINARY
- Called from (representative examples):
  - ActivateCommitTs
  - MaybeExtendOffsetSlru
  - find_multixact_start
  - test_slru_page_exists

## Notes and Other Information
- Returns false for both non-existent files and files too small to contain the requested page
- Uses transient file management to avoid keeping files open unnecessarily
- Implements proper error reporting through SlruReportIOError for genuine I/O failures
- Critical for multixact and commit timestamp systems where page existence affects system behavior
- Part of PostgreSQL's defensive programming approach - verify before attempting operations
- Updates statistics counters to help monitor SLRU subsystem activity and performance