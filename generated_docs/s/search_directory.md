# search_directory

## Location
[src/bin/pg_waldump/pg_waldump.c:210-291](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_waldump/pg_waldump.c#L210-L291)

## Overview
Searches for a specific WAL file or any valid WAL file in a given directory, opens it, and extracts the WAL segment size from the file header.

## Definition
```c
static bool search_directory(const char *directory, const char *fname)
```

## Detailed Description
This function performs directory-based WAL file search operations with two main modes of operation. When a specific filename is provided, it attempts to open that file directly. When no filename is provided (fname is NULL), it scans the entire directory looking for any file with a valid WAL filename pattern.

Upon successfully opening a WAL file, the function reads the first XLOG_BLCKSZ bytes to extract the WAL segment size from the XLogLongPageHeader structure. It validates this segment size and sets the global WalSegSz variable. The function ensures data integrity by performing comprehensive error checking on file operations and WAL segment size validation.

This is a critical function for the pg_waldump utility as it establishes the correct WAL segment size needed for proper WAL file analysis and ensures that the tool can locate and process valid WAL files.

## Parameters / Member Variables
- `directory`: A null-terminated string specifying the directory path to search for WAL files
- `fname`: A null-terminated string specifying the specific filename to search for, or NULL to search for any valid WAL file

## Dependencies
- Functions called/Symbols referenced:
  - [open_file_in_directory](../o/open_file_in_directory.md)
  - opendir
  - readdir
  - closedir
  - [IsXLogFileName](../I/IsXLogFileName.md)
  - read
  - close
  - IsValidWalSegSize
  - pg_log_error
  - pg_log_error_detail
  - ngettext
  - [pg_strdup](../p/pg_strdup.md)
- Called from (representative examples):
  - [identify_target_directory](../i/identify_target_directory.md)

## Notes and Other Information
- Returns true if a valid WAL file is found and successfully processed, false otherwise
- Sets the global WalSegSz variable based on the WAL file header information
- Validates WAL segment size must be a power of two between 1 MB and 1 GB
- Uses PGAlignedXLogBlock for proper memory alignment when reading WAL data
- Part of the pg_waldump utility's file discovery and validation system
- Terminates program execution if invalid WAL segment size is detected or file read operations fail