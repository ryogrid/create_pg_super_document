# close_target_file

## Location
[src/bin/pg_rewind/file_ops.c:75-87](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/file_ops.c#L75-L87)

## Overview
Closes the currently open target file in pg_rewind's file operations system.

## Definition


## Detailed Description
This function safely closes the currently open target file managed by the file operations module. It checks if a file is actually open (dstfd != -1) before attempting to close it, making it safe to call multiple times. Upon successful closure, it resets the global file descriptor to -1 to indicate no file is currently open. If the close operation fails, it terminates the program with a fatal error message including the file path and system error details.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - close (system call)
  - [pg_fatal](../p/pg_fatal.md)
- Global variables used:
  - dstfd (static file descriptor)
  - dstpath (static path buffer for error reporting)
- Called from (representative examples):
  - [open_target_file](../o/open_target_file.md)
  - [perform_rewind](../p/perform_rewind.md)
  - [createBackupLabel](createBackupLabel.md)

## Notes and Other Information
- Part of pg_rewind utility's file operations module (src/bin/pg_rewind/file_ops.c)
- Essential for proper resource management and avoiding file descriptor leaks
- Safe to call even when no file is open (idempotent operation)
- Always called before opening a new target file to ensure clean state
- Critical for maintaining file system consistency during pg_rewind operations
- Uses pg_fatal for error handling, ensuring immediate termination on close failures