# OpenTransientFilePerm

## Location
[src/backend/storage/file/fd.c:2639-2682](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L2639-L2682)

## Overview
OpenTransientFilePerm opens files using raw file descriptors with explicit permission control, providing PostgreSQL's managed alternative to the open() system call.

## Definition

```c
int
OpenTransientFilePerm(const char *fileName, int fileFlags, mode_t fileMode)
```
## Detailed Description
OpenTransientFilePerm serves as PostgreSQL's managed wrapper around file opening operations, similar to AllocateFile but returning an unbuffered file descriptor instead of a FILE* stream. This function integrates with PostgreSQL's file descriptor management system to handle resource constraints and provides transaction-aware cleanup.

Unlike AllocateFile which uses stdio buffering, this function provides direct access to the underlying file descriptor, making it suitable for operations that require precise control over I/O operations or when buffering is not desired. The function automatically manages file descriptor limits by closing least-recently-used files when necessary and ensures proper cleanup during transaction boundaries.

## Parameters / Member Variables
- : The path to the file to be opened
- : File access and creation flags (O_RDONLY, O_WRONLY, O_RDWR, O_CREAT, etc.)
- : File permission mode (e.g., 0600, 0644) used when creating new files

## Dependencies
- Functions called/Symbols referenced:
  - DO_DB (debug logging macro)
  - [reserveAllocatedDesc](../r/reserveAllocatedDesc.md) (reserves an allocated descriptor slot)
  - [ReleaseLruFiles](../R/ReleaseLruFiles.md) (closes least-recently-used files to free FDs)
  - [BasicOpenFilePerm](../B/BasicOpenFilePerm.md) (performs the actual file opening with permission handling)
  - [GetCurrentSubTransactionId](../G/GetCurrentSubTransactionId.md) (tracks which subtransaction opened the file)
  - mode_t (POSIX file permission type)
- Called from (representative examples):
  - [be_lo_export](../b/be_lo_export.md) (be-fsstubs.c:510)
  - [OpenTransientFile](OpenTransientFile.md) (fd.c:2632)

## Notes and Other Information
- Returns a raw file descriptor (int) rather than a FILE* pointer, providing unbuffered access
- Part of PostgreSQL's transient file management system for temporary or short-lived file operations
- Files opened with this function are automatically tracked and closed during transaction commit/abort
- The function implements the same resource management strategy as AllocateFile, including FD limit handling
- All opened file descriptors are stored in the allocatedDescs array with metadata including the creating subtransaction ID
- Returns -1 on failure, following standard UNIX convention for file operations
- The 'Perm' suffix indicates this variant allows explicit permission specification for file creation