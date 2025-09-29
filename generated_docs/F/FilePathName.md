# FilePathName

## Location
[src/backend/storage/file/fd.c:2458-2473](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L2458-L2473)

## Overview
FilePathName is a utility function that returns the pathname associated with an open file descriptor in PostgreSQL's virtual file descriptor system.

## Definition

```c
char *
FilePathName(File file)
```
## Detailed Description
FilePathName provides access to the pathname string associated with an open PostgreSQL File descriptor. The function retrieves the fileName field from the VfdCache (Virtual File Descriptor Cache) for the given file descriptor. This function is part of PostgreSQL's file descriptor management system that abstracts file operations and provides a centralized way to track open files.

The returned string points to an internal buffer that remains valid until the file is closed. This design avoids unnecessary string copying while providing access to file path information needed for logging, error reporting, and backup operations.

## Parameters / Member Variables
- : A PostgreSQL File descriptor representing an open file in the virtual file descriptor system

## Dependencies
- Functions called/Symbols referenced:
  - FileIsValid (validates the file descriptor)
  - VfdCache (global virtual file descriptor cache array)
- Called from (representative examples):
  - [PrepareForIncrementalBackup](../P/PrepareForIncrementalBackup.md) (incremental backup operations)
  - [bbsink_server_archive_contents](../b/bbsink_server_archive_contents.md) (backup sink operations)
  - [BufFileLoadBuffer](../B/BufFileLoadBuffer.md) (buffered file I/O operations)
  - [mdextend](../m/mdextend.md) (magnetic disk storage manager operations)
  - [mdreadv](../m/mdreadv.md)/mdwritev (storage manager read/write operations)

## Notes and Other Information
- The function includes an assertion to validate the file descriptor using FileIsValid
- The returned string is from an internal buffer and should not be modified or freed by the caller
- The pathname remains valid only while the file remains open in the VFD system
- This function is commonly used in error reporting and logging to provide meaningful file information
- Part of PostgreSQL's abstraction layer over operating system file operations

## Simplified Source

```c
char *FilePathName(File file)
{
    // Ensure the file descriptor is valid
    Assert(FileIsValid(file));

    // Return the filename from the virtual file descriptor cache
    return VfdCache[file].fileName;
}
```