# link

## Location
[src/timezone/zic.c:4006-4015](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L4006-L4015)

## Overview
Provides a Windows-compatible implementation of the Unix link() system call for creating hard links between files.

## Definition
```c
int link(const char *src, const char *dst)
```

## Detailed Description
The `link` function implements the standard Unix link() system call on Windows platforms where this functionality is not natively available in the same form. It creates a hard link from the destination path to the source file using the Windows API.

The function serves as a compatibility layer that:
1. Uses the Windows `CreateHardLinkA` API to create the hard link
2. Maps Windows error codes to Unix-style errno values using `_dosmaperr`
3. Returns standard Unix link() return values (0 for success, -1 for failure)
4. Follows the Windows API parameter order (destination first, then source)

This implementation ensures that PostgreSQL code can use the standard link() interface consistently across Unix and Windows platforms.

## Parameters / Member Variables
- `src`: Source file path - the existing file to which the hard link will point
- `dst`: Destination path - the new hard link name to be created

## Dependencies
- Functions called/Symbols referenced:
  - `CreateHardLinkA`: Windows API function for creating hard links
  - `[_dosmaperr](../d/_dosmaperr.md)`: Function to map Windows error codes to errno values
  - `GetLastError`: Windows API function to retrieve last error code
- Called from (representative examples):
  - Various PostgreSQL components including file operations, memory management, hash tables, and timezone utilities

## Notes and Other Information
- Part of PostgreSQLs Windows portability layer in src/port/
- Only compiled and used on Windows platforms
- Uses the ANSI version (CreateHardLinkA) rather than Unicode version of the Windows API
- Hard links on Windows require NTFS filesystem and appropriate permissions
- The function interface matches the standard Unix link() system call exactly
- Error handling follows Unix conventions with errno being set appropriately
- Note: There is also a different link() function in src/timezone/zic.c for WIN32 that uses CopyFile instead of CreateHardLinkA

## Simplified Source

```c
// Simplified version of link - Windows hard link implementation
int link(const char *src, const char *dst) {
    // Create hard link using Windows API (dst -> src)
    if (CreateHardLinkA(dst, src, NULL) == 0) {
        // Map Windows error to Unix errno and return failure
        _dosmaperr(GetLastError());
        return -1;
    }

    // Success
    return 0;
}
```

Key simplifications made:
- Added clear comments explaining the core logic
- Highlighted the parameter order (dst, src) which differs from intuition
- Simplified the else branch to focus on the success path
- Emphasized the error mapping mechanism