# FreeDesc

## Location
[src/backend/storage/file/fd.c:2739-2777](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L2739-L2777)

## Overview
FreeDesc is an internal static function that closes and deallocates file descriptors managed by PostgreSQL's file descriptor allocation system.

## Definition

```c
static int
FreeDesc(AllocateDesc *desc)
```
## Detailed Description
FreeDesc serves as the central cleanup function for all types of allocated file descriptors in PostgreSQL's file management system. This static function handles the proper closure of different descriptor types (files, pipes, directories, and raw file descriptors) by dispatching to the appropriate system call based on the descriptor's kind. After closing the underlying resource, it compacts the allocatedDescs array to maintain efficient memory usage.

The function is designed to be type-aware, supporting the four different kinds of descriptors that PostgreSQL tracks: regular files (FILE*), pipe streams, directory handles, and raw file descriptors. This unified interface allows higher-level functions to close any type of descriptor without needing to know its specific type.

## Parameters / Member Variables
- : Pointer to an AllocateDesc structure in the allocatedDescs array that should be freed

## Dependencies
- Functions called/Symbols referenced:
  - AllocateDesc (the descriptor structure type)
  - fclose (for closing regular files - AllocateDescFile)
  - [pclose](../p/pclose.md) (for closing pipe streams - AllocateDescPipe)  
  - [closedir](../c/closedir.md) (for closing directories - AllocateDescDir)
  - close (for closing raw file descriptors - AllocateDescRawFD)
  - elog (for error reporting when descriptor kind is unrecognized)
- Called from (representative examples):
  - [FreeFile](FreeFile.md) (fd.c:2790)
  - [CloseTransientFile](../C/CloseTransientFile.md) (fd.c:2818)
  - [ClosePipeStream](../C/ClosePipeStream.md) (fd.c:3000)
  - [AtEOSubXact_Files](../A/AtEOSubXact_Files.md) (fd.c:3143)
  - [CleanupTempFiles](../C/CleanupTempFiles.md) (fd.c:3246)

## Notes and Other Information
- This is a static function, only accessible within fd.c
- The desc parameter must point to a valid entry in the allocatedDescs[] array
- After closing the underlying resource, the function compacts the array by moving the last element to the freed position
- Returns the result of the underlying close operation (0 for success, -1 for failure typically)
- Supports four descriptor kinds: AllocateDescFile, AllocateDescPipe, AllocateDescDir, and AllocateDescRawFD
- The function decrements numAllocatedDescs to maintain the correct count of active descriptors
- Used internally by all public descriptor closing functions to ensure consistent cleanup behavior
- Critical for preventing resource leaks in PostgreSQL's file descriptor management system