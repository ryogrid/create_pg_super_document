# FileGetRawMode

## Location
[src/backend/storage/file/fd.c:2494-2504](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L2494-L2504)

## Overview
FileGetRawMode returns the file mode (permission bits) that were used when creating a PostgreSQL File, providing access to the original mode parameter from the open(2) system call.

## Definition

```c
mode_t
FileGetRawMode(File file)
```
## Detailed Description
FileGetRawMode retrieves the fileMode field from the VfdCache for a given PostgreSQL File descriptor. This mode represents the permission bits (file mode) that were passed to the open(2) system call when the file was created or opened, such as 0644, 0600, or other Unix permission combinations. The mode parameter is only meaningful when creating new files (when O_CREAT flag is used) and determines the initial permissions of the created file.

The function provides access to the cached mode without requiring additional system calls, as PostgreSQL stores this information in its virtual file descriptor cache when files are opened through the VFD system.

## Parameters / Member Variables
- : A PostgreSQL File descriptor representing an open file in the virtual file descriptor system

## Dependencies
- Functions called/Symbols referenced:
  - FileIsValid (validates the file descriptor)
  - VfdCache (global virtual file descriptor cache array)
- Called from (representative examples):
  - PG_O_DIRECT (for file mode information in direct I/O contexts)

## Notes and Other Information
- The function includes an assertion to validate the file descriptor using FileIsValid
- The returned mode is of type mode_t, which represents Unix file permissions
- The mode is only significant when the file was created (O_CREAT flag was used)
- Common modes include 0644 (rw-r--r--), 0600 (rw-------), 0755 (rwxr-xr-x)
- This function is part of PostgreSQL's file descriptor introspection capabilities
- The mode remains constant for the lifetime of the file descriptor
- Used primarily for auditing file permissions and ensuring proper access controls