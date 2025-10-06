# _pgfstat64

## Location
[src/port/win32stat.c:255-302](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/win32stat.c#L255-L302)

## Overview
Windows-specific implementation of the fstat() system call that retrieves file status information for a given file descriptor.

## Definition

```c
int
_pgfstat64(int fileno, struct stat *buf)
```
## Detailed Description
The  function provides a Windows-compatible implementation of the POSIX fstat() system call. It takes a file descriptor number and fills a stat structure with file information. The function uses Windows API calls to determine the file type and populate the stat structure accordingly.

The function handles different Windows file types:
- **FILE_TYPE_DISK**: Regular disk files - delegates to  for complete file information
- **FILE_TYPE_PIPE**: Named pipes, anonymous pipes, and sockets - sets mode to 
- **FILE_TYPE_CHAR**: Character devices - sets mode to 
- **FILE_TYPE_REMOTE/FILE_TYPE_UNKNOWN**: Returns error for unsupported file types

For non-disk files (pipes and character devices), the function sets minimal stat information including the file mode, device numbers set to the file descriptor number, and link count of 1.

## Parameters / Member Variables
- `fileno`: File descriptor number obtained from file operations
- `*buf`: Pointer to a struct stat that will be filled with file status information
## Dependencies
- Functions called/Symbols referenced:
  - [pgwin32_get_file_type](pgwin32_get_file_type.md)
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

## Simplified Source

```c
int
_pgfstat64(int fileno, struct stat *buf)
{
    HANDLE hFile = (HANDLE) _get_osfhandle(fileno);
    unsigned short st_mode;

    if (buf == NULL)
    {
        errno = EINVAL;
        return -1;
    }

    // Determine the file type using Windows API
    DWORD fileType = pgwin32_get_file_type(hFile);
    if (errno != 0)
        return -1;

    switch (fileType)
    {
        case FILE_TYPE_DISK:
            // For disk files, get complete file information
            return fileinfo_to_stat(hFile, buf);

        case FILE_TYPE_PIPE:
            // Named pipes, anonymous pipes, or sockets
            st_mode = _S_IFIFO;
            break;

        case FILE_TYPE_CHAR:
            // Character devices
            st_mode = _S_IFCHR;
            break;

        case FILE_TYPE_REMOTE:
        case FILE_TYPE_UNKNOWN:
        default:
            // Unsupported file types
            errno = EINVAL;
            return -1;
    }

    // Fill stat structure with minimal information for special files
    memset(buf, 0, sizeof(*buf));
    buf->st_mode = st_mode;
    buf->st_dev = fileno;
    buf->st_rdev = fileno;
    buf->st_nlink = 1;
    return 0;
}
```