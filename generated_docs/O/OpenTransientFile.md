# OpenTransientFile

## Location
[src/backend/storage/file/fd.c:2630-2638](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L2630-L2638)

## Overview
OpenTransientFile is a convenience wrapper that opens a file with default permissions using PostgreSQL's transient file management system.

## Definition

```c
int
OpenTransientFile(const char *fileName, int fileFlags)
```
## Detailed Description
OpenTransientFile provides a simplified interface for opening files through PostgreSQL's managed file system. It serves as a wrapper around OpenTransientFilePerm(), automatically passing the default file creation mode (pg_file_create_mode) as the permission parameter. This function is part of PostgreSQL's file descriptor management system, which handles resource limits and provides transaction-aware file operations.

The function is designed for files that need to be opened with standard PostgreSQL default permissions without requiring explicit permission specification from the caller.

## Parameters / Member Variables
- : The path to the file to be opened
- : File access flags (O_RDONLY, O_WRONLY, O_RDWR, etc., similar to open() system call)

## Dependencies
- Functions called/Symbols referenced:
  - [OpenTransientFilePerm](OpenTransientFilePerm.md) (the underlying function that performs the actual file opening)
  - pg_file_create_mode (global variable containing default file creation permissions)
- Called from (representative examples):
  - [heap_xlog_logical_rewrite](../h/heap_xlog_logical_rewrite.md) (rewriteheap.c:1089)
  - [SlruPhysicalReadPage](../S/SlruPhysicalReadPage.md) (slru.c:819)
  - [writeTimeLineHistory](../w/writeTimeLineHistory.md) (timeline.c:325)
  - [sendFile](../s/sendFile.md) (basebackup.c:1591)
  - [durable_rename](../d/durable_rename.md) (fd.c:793)

## Notes and Other Information
- This is essentially a convenience function that reduces code duplication by providing default file permissions
- Returns a file descriptor (int) rather than a FILE* pointer, unlike AllocateFile
- Part of PostgreSQL's transient file management system, designed for temporary or short-lived file operations
- Files opened with this function should be closed using the appropriate PostgreSQL file management functions
- The actual file opening logic and resource management is handled by OpenTransientFilePerm

## Simplified Source

```c
// Simplified version of OpenTransientFile
int OpenTransientFile(const char *fileName, int fileFlags) {
    // Open file with default PostgreSQL file creation permissions
    return OpenTransientFilePerm(fileName, fileFlags, pg_file_create_mode);
}
```

Key simplifications made:
- Added comment explaining the default permission usage
- Function is already very simple, minimal changes needed
- Preserved the essential wrapper functionality
- Maintained the delegation to OpenTransientFilePerm with default mode