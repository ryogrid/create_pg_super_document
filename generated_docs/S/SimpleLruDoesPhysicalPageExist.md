# SimpleLruDoesPhysicalPageExist

## Location
[src/backend/access/transam/slru.c:743-800](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/slru.c#L743-L800)

## Overview
Determines whether a specific SLRU page exists on disk by checking file existence and size, used for validation before attempting read operations.

## Definition

```c
bool
SimpleLruDoesPhysicalPageExist(SlruCtl ctl, int64 pageno)
```
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
- `ctl`: SlruCtl control structure containing SLRU configuration and shared state information
- `pageno`: 64-bit logical page number to check for existence on disk
## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_count_slru_page_exists](../p/pgstat_count_slru_page_exists.md)
  - [SlruFileName](SlruFileName.md)
  - [OpenTransientFile](../O/OpenTransientFile.md)
  - lseek
  - [CloseTransientFile](../C/CloseTransientFile.md)
  - [SlruReportIOError](SlruReportIOError.md)
  - SLRU_PAGES_PER_SEGMENT
  - PG_BINARY
- Called from (representative examples):
  - [ActivateCommitTs](../A/ActivateCommitTs.md)
  - [MaybeExtendOffsetSlru](../M/MaybeExtendOffsetSlru.md)
  - [find_multixact_start](../f/find_multixact_start.md)
  - [test_slru_page_exists](../t/test_slru_page_exists.md)

## Notes and Other Information
- Returns false for both non-existent files and files too small to contain the requested page
- Uses transient file management to avoid keeping files open unnecessarily
- Implements proper error reporting through SlruReportIOError for genuine I/O failures
- Critical for multixact and commit timestamp systems where page existence affects system behavior
- Part of PostgreSQL's defensive programming approach - verify before attempting operations
- Updates statistics counters to help monitor SLRU subsystem activity and performance

## Simplified Source

```c
// Simplified version of SimpleLruDoesPhysicalPageExist
bool SimpleLruDoesPhysicalPageExist(SlruCtl ctl, int64 pageno) {
    // Calculate segment and page offset
    int64 segno = pageno / SLRU_PAGES_PER_SEGMENT;
    int rpageno = pageno % SLRU_PAGES_PER_SEGMENT;
    int offset = rpageno * BLCKSZ;
    char path[MAXPGPATH];
    int fd;
    bool result;
    off_t endpos;

    // Update statistics
    pgstat_count_slru_page_exists(ctl->shared->slru_stats_idx);

    // Get segment file path
    SlruFileName(ctl, path, segno);

    // Try to open file
    fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
    if (fd < 0) {
        if (errno == ENOENT)
            return false;  // File doesn't exist - normal case

        // Report actual error
        slru_errcause = SLRU_OPEN_FAILED;
        slru_errno = errno;
        SlruReportIOError(ctl, pageno, 0);
    }

    // Check if file is large enough to contain the page
    if ((endpos = lseek(fd, 0, SEEK_END)) < 0) {
        slru_errcause = SLRU_SEEK_FAILED;
        slru_errno = errno;
        SlruReportIOError(ctl, pageno, 0);
    }

    result = endpos >= (off_t)(offset + BLCKSZ);

    // Clean up file handle
    if (CloseTransientFile(fd) != 0) {
        slru_errcause = SLRU_CLOSE_FAILED;
        slru_errno = errno;
        return false;
    }

    return result;
}
```

Key simplifications made:
- Added clear comments for each major step
- Grouped related operations together logically
- Simplified error handling explanation while preserving all error paths
- Focused on the core algorithm: segment calculation, file opening, size checking
- Maintained all essential logic and error reporting behavior