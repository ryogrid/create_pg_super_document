# ws_file_info

## Location
src/bin/pg_walsummary/pg_walsummary.c: 31 - 35

## Overview
The `ws_file_info` structure encapsulates file information for WAL summary file operations, storing both the file descriptor and filename for file I/O operations in the `pg_walsummary` utility.

## Definition
```c
typedef struct ws_file_info
{
    int         fd;
    char       *filename;
} ws_file_info;
```

## Detailed Description
The `ws_file_info` structure serves as a container for file-related information needed during WAL summary file processing. It combines a file descriptor with the corresponding filename, providing both the low-level file access handle and human-readable file identification for error reporting and logging purposes.

This structure is primarily used as callback argument data in file reading operations, where functions need access to both the file descriptor for actual I/O operations and the filename for meaningful error messages. The design follows a common pattern in PostgreSQL utilities where file operations require both efficient access and good error reporting.

## Parameters / Member Variables
- `fd`: Integer file descriptor used for low-level file read operations (obtained from `open()` system call)
- `filename`: Pointer to null-terminated string containing the name of the file being processed (used primarily for error reporting and logging)

## Dependencies
- Functions called/Symbols referenced: None (simple data structure)
- Called from (representative examples):
  - `[walsummary_read_callback](walsummary_read_callback.md)` function at src/bin/pg_walsummary/pg_walsummary.c:248 (used as callback argument to access file information during read operations)

## Notes and Other Information
- The structure is used as callback argument data (`callback_arg`) in the WAL summary file reading infrastructure
- The `filename` member is critical for providing meaningful error messages when file operations fail
- Memory management for the `filename` string is handled by the calling code
- This structure enables the separation of concerns where the callback function can perform I/O operations while maintaining access to file metadata
- Part of the broader WAL summary file processing system in PostgreSQL which is used for incremental backup and recovery features