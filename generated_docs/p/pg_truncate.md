# pg_truncate

## Location
[src/backend/storage/file/fd.c:717-752](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L717-L752)

## Overview
A PostgreSQL function that truncates a file to a specified length by file path, with platform-specific implementations for Windows and Unix systems.

## Definition
```c
int pg_truncate(const char *path, off_t length)
```

## Detailed Description
The pg_truncate function provides a cross-platform interface to truncate a file to a given length using the file's path. The function has two distinct implementations based on the target platform:

**Windows Implementation**: Since Windows lacks a direct path-based truncate system call, the function opens the file using PostgreSQL's OpenTransientFile(), calls pg_ftruncate() on the file descriptor, and then properly closes the file while preserving the errno value from the truncate operation.

**Unix Implementation**: Uses the system truncate() call directly with the file path, implementing retry logic to handle EINTR signal interruptions gracefully.

Both implementations ensure reliable file truncation with proper error handling and signal interrupt recovery.

## Parameters / Member Variables
- `path`: File path of the file to truncate
- `length`: The desired length to truncate the file to (in bytes)

## Dependencies
- Functions called/Symbols referenced:
  - [OpenTransientFile](../O/OpenTransientFile.md) (Windows)
  - [pg_ftruncate](pg_ftruncate.md) (Windows)
  - [CloseTransientFile](../C/CloseTransientFile.md) (Windows)
  - truncate (Unix)
  - PG_BINARY
  - EINTR
- Called from (representative examples):
  - [do_truncate](../d/do_truncate.md)
  - PG_O_DIRECT (referenced in fd.h)

## Notes and Other Information
- Returns 0 on success, -1 on error with errno set appropriately  
- Handles platform differences transparently to callers
- On Windows, properly manages file descriptor lifecycle and errno preservation
- On Unix, implements automatic retry on EINTR signal interruption
- Part of PostgreSQL's cross-platform file management system
- Used primarily in storage management operations like relation truncation