# AllocateFile

## Location
[src/backend/storage/file/fd.c:2580-2629](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L2580-L2629)

## Overview
AllocateFile is PostgreSQL's managed wrapper around the standard C library fopen() function, providing automatic file descriptor management and transaction-aware cleanup.

## Definition

```c
FILE *
AllocateFile(const char *name, const char *mode)
```
## Detailed Description
AllocateFile serves as the primary interface for opening files using stdio (FILE*) within the PostgreSQL backend. Unlike direct fopen() calls, this function integrates with PostgreSQL's file descriptor management system to prevent resource exhaustion. It automatically handles closing of least-recently-used files when file descriptor limits are reached, and ensures all opened files are properly closed during transaction commit or abort to prevent file descriptor leakage.

The function is specifically designed for short-lived file operations, such as reading configuration files that will be immediately closed. Files intended to remain open for extended periods should not use this mechanism as they cannot share kernel file descriptors with other files, risking FD exhaustion.

## Parameters / Member Variables
- : The path to the file to be opened
- : The file opening mode string (same as standard fopen() modes: "r", "w", "a", etc.)

## Dependencies
- Functions called/Symbols referenced:
  - DO_DB (debug logging macro)
  - [reserveAllocatedDesc](../r/reserveAllocatedDesc.md) (reserves an allocated descriptor slot)
  - [ReleaseLruFiles](../R/ReleaseLruFiles.md) (closes least-recently-used files to free FDs)
  - fopen (standard C library file opening function)
  - [GetCurrentSubTransactionId](../G/GetCurrentSubTransactionId.md) (tracks which subtransaction opened the file)
  - [ReleaseLruFile](../R/ReleaseLruFile.md) (releases a single LRU file when retrying after EMFILE/ENFILE)
- Called from (representative examples):
  - [readTimeLineHistory](../r/readTimeLineHistory.md) (timeline.c:105)
  - [BeginCopyFrom](../B/BeginCopyFrom.md) (copyfrom.c:1731)
  - [parse_extension_control_file](../p/parse_extension_control_file.md) (extension.c:493)
  - [open_auth_file](../o/open_auth_file.md) (hba.c:617)
  - [load_relcache_init_file](../l/load_relcache_init_file.md) (relcache.c:6095)

## Notes and Other Information
- This should be the only direct call to fopen() in the PostgreSQL backend
- Files opened with AllocateFile must be closed with FreeFile, not fclose()
- All files are automatically closed at transaction commit/abort for cleanup
- The function implements retry logic when encountering EMFILE/ENFILE errors
- Maximum allocated descriptors is controlled by maxAllocatedDescs parameter
- Each opened file is tracked in the allocatedDescs array with metadata including the creating subtransaction ID