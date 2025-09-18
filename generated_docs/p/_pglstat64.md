# _pglstat64

## Location
[src/port/win32stat.c:113-197](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/win32stat.c#L113-L197)

## Overview
Windows implementation of the lstat() system call that retrieves file status information, including proper handling of junction points as symbolic links.

## Definition
```c
int _pglstat64(const char *name, struct stat *buf)
```

## Detailed Description
This function provides a Windows-compatible implementation of the Unix lstat() system call. Unlike stat(), lstat() does not follow symbolic links but instead returns information about the link itself. On Windows, this mainly applies to junction points, which are treated as symbolic links.

The function implements a sophisticated approach to handle Windows junction points:
1. First attempts to open the file/directory using pgwin32_open_handle()
2. If successful, uses fileinfo_to_stat() to get basic file information
3. For directories or when the initial open fails with ENOENT, it uses readlink() to check if the target is a junction point
4. If readlink() succeeds, the entry is treated as a symbolic link (S_IFLNK) with st_size set to the target path length

Special handling includes:
- Junction points pointing to non-existent paths (distinguishing from true ENOENT)
- STATUS_DELETE_PENDING conditions (files being deleted)
- Proper cleanup of file handles

## Parameters / Member Variables
- `name`: Path to the file or directory to examine
- `buf`: Pointer to struct stat structure to be filled with file information

## Dependencies
- Functions called/Symbols referenced:
  - [pgwin32_open_handle](pgwin32_open_handle.md) (PostgreSQL Windows file opening wrapper)
  - [fileinfo_to_stat](../f/fileinfo_to_stat.md) (convert Windows file info to stat structure)
  - readlink (check for junction points/symbolic links)
  - CloseHandle (Windows API for handle cleanup)
  - S_ISDIR, S_IFLNK (Unix file type macros)
  - pg_RtlGetLastNtStatus (Windows NT status checking)
- Called from:
  - [stat](../s/stat.md) (macro in src/include/port/win32_port.h:279)
  - lstat (macro in src/include/port/win32_port.h:283)
  - [_pgstat64](_pgstat64.md) (at src/port/win32stat.c:204, 244)

## Notes and Other Information
- Returns 0 on success, -1 on error (following Unix conventions)
- Uses FILE_FLAG_BACKUP_SEMANTICS to allow opening directories
- Handles Windows junction points by converting them to symbolic link entries
- Part of PostgreSQL's Windows portability layer
- The function name includes '64' suffix for 64-bit file size support
- Properly handles edge cases like files being deleted while being accessed
- Uses private handle-based operations to avoid running out of file descriptors