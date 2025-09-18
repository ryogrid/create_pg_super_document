# _pgfstat64

## Location
[src/port/win32stat.c:255-302](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/win32stat.c#L255-L302)

## Overview
Windows-specific implementation of the fstat() system call that retrieves file status information for a given file descriptor.

## Definition


## Detailed Description
The  function provides a Windows-compatible implementation of the POSIX fstat() system call. It takes a file descriptor number and fills a stat structure with file information. The function uses Windows API calls to determine the file type and populate the stat structure accordingly.

The function handles different Windows file types:
- **FILE_TYPE_DISK**: Regular disk files - delegates to  for complete file information
- **FILE_TYPE_PIPE**: Named pipes, anonymous pipes, and sockets - sets mode to 
- **FILE_TYPE_CHAR**: Character devices - sets mode to 
- **FILE_TYPE_REMOTE/FILE_TYPE_UNKNOWN**: Returns error for unsupported file types

For non-disk files (pipes and character devices), the function sets minimal stat information including the file mode, device numbers set to the file descriptor number, and link count of 1.

## Parameters / Member Variables
- : File descriptor number obtained from file operations
- : Pointer to a struct stat that will be filled with file status information

## Dependencies
- Functions called/Symbols referenced:
  - pgwin32_get_file_type
  - [fileinfo_to_stat](../f/fileinfo_to_stat.md)
- Called from (representative examples):
  - [stat](../s/stat.md) (via macro in src/include/port/win32_port.h:277)
  - fstat (via macro in src/include/port/win32_port.h:281)

## Notes and Other Information
- This is a Windows-specific implementation located in src/port/win32stat.c:255-302
- Returns 0 on success, -1 on error with errno set appropriately
- Part of PostgreSQL's platform abstraction layer for Windows compatibility
- Uses Windows API  to convert file descriptor to Windows HANDLE
- For disk files, delegates to  which provides complete file metadata
- For special file types (pipes, character devices), provides minimal but sufficient stat information