# ReportTemporaryFileUsage

## Location
[src/backend/storage/file/fd.c:1525-1543](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L1525-L1543)

## Overview
ReportTemporaryFileUsage is a static function that reports the usage of temporary files for statistical tracking and logging purposes when temporary files are deleted.

## Definition

```c
static void
ReportTemporaryFileUsage(const char *path, off_t size)
```
## Detailed Description
ReportTemporaryFileUsage is called whenever a temporary file is deleted to report its size for both statistical tracking and optional logging. The function serves two main purposes:

1. **Statistics Reporting**: Always reports the temporary file size to the PostgreSQL statistics system via pgstat_report_tempfile(), which tracks temporary file usage across the database system.

2. **Optional Logging**: Conditionally logs temporary file information based on the log_temp_files configuration parameter. If log_temp_files is set to a non-negative value, files larger than the specified threshold (in KB) will be logged at the LOG level, showing the file path and size.

The logging helps database administrators monitor temporary file usage patterns and identify queries or operations that are creating large temporary files, which can indicate performance issues or suboptimal query plans.

## Parameters / Member Variables
- `*path`: The file system path of the temporary file that was deleted
- `size`: The size of the deleted temporary file in bytes (off_t type)
## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_report_tempfile](../p/pgstat_report_tempfile.md) (function to report temporary file statistics)
- Called from (representative examples):
  - [PathNameDeleteTemporaryFile](../P/PathNameDeleteTemporaryFile.md)
  - [FileClose](../F/FileClose.md)

## Notes and Other Information
- This is a static function internal to fd.c, not exposed in the public API
- The function is called during temporary file cleanup operations
- The log_temp_files parameter controls the logging threshold in kilobytes
- Setting log_temp_files to 0 logs all temporary files
- Setting log_temp_files to -1 disables temporary file logging
- The size comparison uses integer division (size / 1024) to convert bytes to KB
- Statistics are always reported regardless of the logging configuration
- Helps with monitoring and troubleshooting temporary file usage in PostgreSQL

## Simplified Source

```c
// Simplified version of ReportTemporaryFileUsage
static void ReportTemporaryFileUsage(const char *path, off_t size) {
    // Always report temporary file statistics
    pgstat_report_tempfile(size);

    // Log temporary file info if logging is enabled and size meets threshold
    if (log_temp_files >= 0) {
        if ((size / 1024) >= log_temp_files) {
            ereport(LOG, (errmsg("temporary file: path \"%s\", size %lu",
                                path, (unsigned long) size)));
        }
    }
}
```

Key simplifications made:
- Preserved the dual purpose: statistics reporting and optional logging
- Maintained the size threshold check in kilobytes
- Kept the conditional logging based on log_temp_files configuration
- Added comments explaining the two main operations
- Simple and clear structure with no unnecessary complexity