# FileSize

## Location
[src/backend/storage/file/fd.c:2406-2422](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L2406-L2422)

## Overview
FileSize determines the current size of a file in bytes by seeking to the end of the file and returning the position.

## Definition
```c
off_t FileSize(File file)
```

## Detailed Description
FileSize efficiently determines the size of a file by using the lseek() system call to seek to the end of the file (SEEK_END) and returning the resulting position, which represents the file size in bytes. The function validates the file descriptor and ensures the file is accessible before performing the size query. If the file is not currently open, it automatically opens the file through the FileAccess() function. This is a lightweight operation that doesn't require reading file contents or metadata beyond what the filesystem provides through the seek operation.

## Parameters / Member Variables
- `file`: Virtual file descriptor representing the file whose size is to be determined

## Dependencies
- Functions called/Symbols referenced:
  - FileIsValid: Validates the virtual file descriptor
  - FileIsNotOpen: Checks if the file is currently open in the VFD cache
  - [FileAccess](FileAccess.md): Opens/accesses the file if it's not currently open
  - lseek: POSIX system call to seek to end of file and get position
  - DO_DB: Debug logging macro
- Called from (representative examples):
  - [BufFileSeek](../B/BufFileSeek.md): Used in buffered file operations for seeking validation
  - [BufFileSize](../B/BufFileSize.md): To determine the size of buffered files
  - [_mdnblocks](../m/_mdnblocks.md): In MD storage manager to calculate the number of blocks in a file

## Notes and Other Information
- Returns the file size in bytes as an off_t value, or -1 on error
- Uses lseek() with SEEK_END to efficiently determine file size without reading file contents
- Automatically handles file opening if the file is not currently open in the VFD cache
- Part of PostgreSQL's Virtual File Descriptor (VFD) system for efficient file handle management
- The file position is moved to the end as a side effect of this operation
- Includes debug logging to track file size queries
- Commonly used for validation and space calculations in storage management operations
- Critical for storage managers to understand file boundaries and plan operations