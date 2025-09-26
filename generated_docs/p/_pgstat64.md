# _pgstat64

## Location
[src/port/win32stat.c:198-254](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/win32stat.c#L198-L254)

## Overview
Windows implementation of the stat() system call that follows symbolic links (junction points) to retrieve information about the target file or directory.

## Definition
```c
int _pgstat64(const char *name, struct stat *buf)
```

## Detailed Description
This function provides a Windows-compatible implementation of the Unix stat() system call. Unlike lstat() which returns information about symbolic links themselves, stat() follows symbolic links to get information about their targets. On Windows, this primarily applies to junction points.

The function implements symbolic link following through an iterative approach:
1. Initially calls _pglstat64() to get basic file information
2. If the result indicates a symbolic link (S_ISLNK), it enters a loop to follow the link
3. Uses readlink() to get the target path of each symbolic link
4. Recursively calls _pglstat64() on each target until a non-link is found
5. Includes protection against infinite loops by limiting traversal to 8 levels

Key features:
- Proper handling of Windows junction points as symbolic links
- Loop detection and prevention (max 8 levels)
- Path length validation to prevent buffer overflows
- Error handling for deleted files and access issues

## Parameters / Member Variables
- `name`: Path to the file or directory to examine (will follow symbolic links)
- `buf`: Pointer to struct stat structure to be filled with target file information

## Dependencies
- Functions called/Symbols referenced:
  - [_pglstat64](_pglstat64.md) (lstat implementation for initial and recursive calls)
  - readlink (read symbolic link target paths)
  - [strlcpy](../s/strlcpy.md) (safe string copying)
  - strcpy (string copying for current path tracking)
  - S_ISLNK (macro to test for symbolic links)
  - pg_RtlGetLastNtStatus (Windows NT status checking)
- Called from:
  - [stat](../s/stat.md) (macro in src/include/port/win32_port.h:278, 282)

## Notes and Other Information
- Returns 0 on success, -1 on error (following Unix conventions)
- Limits symbolic link traversal to 8 levels to prevent infinite loops (returns ELOOP)
- Handles edge cases like files being deleted during traversal (STATUS_DELETE_PENDING)
- Uses MAXPGPATH-sized buffers for path handling
- Part of PostgreSQL's Windows portability layer for file system operations
- The function name includes '64' suffix for 64-bit file size support
- Optimizes by reusing _pglstat64() which already handles junction point detection
- Includes path length validation to prevent ENAMETOOLONG errors