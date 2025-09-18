# copy_file_copyfile

## Location
[src/bin/pg_combinebackup/copy_file.c:294-306](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/copy_file.c#L294-L306)

## Overview
A Windows-specific file copying implementation that uses the Windows CopyFile API for efficient file copying with checksum calculation support.

## Definition


## Detailed Description
The `copy_file_copyfile` function is a Windows-specific implementation for copying files in the pg_combinebackup utility. It leverages the Windows CopyFile API to perform the actual file copy operation, which is typically more efficient than block-by-block copying on Windows systems. The function is conditionally compiled only on Windows platforms (controlled by `#ifdef WIN32`).

The function performs two main operations:
1. Copies the source file to the destination using the Windows CopyFile API with the `bFailIfExists` parameter set to `true`
2. Calculates a checksum of the source file if checksum calculation is enabled

If the CopyFile operation fails, the function maps the Windows error to a POSIX-style error using `_dosmaperr()` and reports a fatal error with details about the failed operation.

## Parameters / Member Variables
- `src`: Source file path to be copied
- `dst`: Destination file path where the source will be copied
- `checksum_ctx`: Checksum context for calculating file checksums; if checksum type is CHECKSUM_TYPE_NONE, no checksum calculation is performed

## Dependencies
- Functions called/Symbols referenced:
  - CopyFile (Windows API function)
  - _dosmaperr (Windows error mapping function)
  - GetLastError (Windows API function)
  - [pg_fatal](../p/pg_fatal.md) (PostgreSQL error reporting function)
  - [checksum_file](checksum_file.md) (internal function for checksum calculation)
- Called from (representative examples):
  - [copy_file](copy_file.md) (via strategy_implementation function pointer when COPY_METHOD_COPYFILE is selected)

## Notes and Other Information
- This function is only available on Windows platforms and is conditionally compiled with `#ifdef WIN32`
- It is used as one of several file copying strategies in pg_combinebackup, specifically selected when COPY_METHOD_COPYFILE is chosen
- The function uses the Windows CopyFile API with `bFailIfExists=true`, meaning it will fail if the destination file already exists
- Checksum calculation is performed on the source file after the copy operation, ensuring data integrity verification
- Error handling includes proper mapping of Windows-specific errors to POSIX-style errors for consistent error reporting across platforms