# check_copy_file_range

## Location
src/bin/pg_upgrade/file.c: 400 - 436

## Overview
Tests the availability and functionality of copy_file_range system call on the platform during pg_upgrade operations.

## Definition


## Detailed Description
This function is part of pg_upgrade's compatibility checking system that verifies whether the copy_file_range() system call is available and working properly on the current platform. The copy_file_range() system call allows efficient copying of data between files within the kernel space without transferring data to user space, making it significantly faster for large file operations.

The function performs a practical test by:
1. Creating test file paths using the old and new cluster data directories
2. Opening the existing PG_VERSION file from the old cluster as source
3. Creating a temporary test file in the new cluster directory as destination
4. Attempting to use copy_file_range() to copy data between the files
5. Cleaning up the temporary test file

If copy_file_range() is not available at compile time (HAVE_COPY_FILE_RANGE not defined), the function immediately fails with a fatal error. This ensures that pg_upgrade operations can reliably use this efficient file copying mechanism when available.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - unlink (system call for removing files)
  - open (system call for opening files)
  - close (system call for closing file descriptors)
  - copy_file_range (Linux system call for efficient file copying)
  - snprintf (for formatting file paths)
  - [pg_fatal](../p/pg_fatal.md) (PostgreSQL error reporting function)
- Global variables accessed:
  - old_cluster.pgdata (source data directory path)
  - new_cluster.pgdata (destination data directory path)
  - pg_file_create_mode (file permission mode)
- Called from:
  - [check_new_cluster](check_new_cluster.md) (src/bin/pg_upgrade/check.c:702)

## Notes and Other Information
- This function is conditional on HAVE_COPY_FILE_RANGE being defined at compile time
- The copy_file_range() system call is primarily available on Linux systems (kernel 4.5+)
- The function uses PG_VERSION file for testing since it's guaranteed to exist and is small
- Failure of this test means pg_upgrade cannot use the efficient copy_file_range mechanism
- The temporary test file name includes '.copy_file_range_test' suffix to identify its purpose
- File descriptors are properly managed with close() calls to prevent resource leaks