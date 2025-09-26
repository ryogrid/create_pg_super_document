# AllocateDescKind

## Location
[src/backend/storage/file/fd.c:253-264](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L253-L264)

## Overview
AllocateDescKind is an enumeration type that categorizes different types of OS handles (file descriptors, file pointers, directory pointers) managed by PostgreSQL's file descriptor management system in fd.c.

## Definition

```c
typedef enum
{
	AllocateDescFile,
	AllocateDescPipe,
	AllocateDescDir,
	AllocateDescRawFD,
} AllocateDescKind;
```
## Detailed Description
AllocateDescKind serves as a discriminator enum within the AllocateDesc structure to identify the type of OS handle being tracked. This enumeration is part of PostgreSQL's file descriptor management system that tracks handles opened with AllocateFile, AllocateDir, OpenPipeStream, and OpenTransientFile functions. The enum enables the system to properly close different types of handles using the appropriate system calls (fclose, pclose, closedir, or close) and to perform type-safe lookups when searching for specific handles.

## Parameters / Member Variables
- `AllocateDescFile`: Indicates the handle is a FILE* opened with AllocateFile() (uses fclose() for cleanup)
- `AllocateDescPipe`: Indicates the handle is a pipe FILE* opened with OpenPipeStream() (uses pclose() for cleanup)
- `AllocateDescDir`: Indicates the handle is a DIR* opened with AllocateDir() (uses closedir() for cleanup)
- `AllocateDescRawFD`: Indicates the handle is a raw file descriptor opened with OpenTransientFile() (uses close() for cleanup)

## Dependencies
- Functions called/Symbols referenced:
  - SubTransactionId (used in AllocateDesc structure)
  - [DIR](../D/DIR.md) (directory pointer type)
- Called from (representative examples):
  - AllocateDesc structure (as kind member at src/backend/storage/file/fd.c:257)
  - [FreeDesc](../F/FreeDesc.md) function (switch statement for proper cleanup)
  - Various search functions (FreeFile, FreeRawFD, FreeDir, FreePipeFile)

## Notes and Other Information
- This enum is central to PostgreSQL's file descriptor tracking mechanism that ensures proper cleanup of OS handles during transaction rollbacks and error conditions
- The enum values correspond directly to different PostgreSQL file management functions and their underlying OS handle types
- Used in switch statements throughout fd.c to dispatch to appropriate cleanup functions (fclose, pclose, closedir, close)
- Part of the AllocateDesc tracking system that maintains a list of all open handles with their subtransaction context for proper resource management