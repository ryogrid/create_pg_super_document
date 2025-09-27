# syslogger_fdget

## Location
[src/backend/postmaster/syslogger.c:802-823](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/syslogger.c#L802-L823)

## Overview
syslogger_fdget is a utility wrapper function that extracts the file descriptor from an opened FILE stream, used for passing file descriptors to the logging collector in EXEC_BACKEND builds.

## Definition

```c
static int
syslogger_fdget(FILE *file)
```
## Detailed Description
syslogger_fdget provides a cross-platform abstraction for obtaining the underlying file descriptor from a FILE pointer. This function is specifically designed for use in EXEC_BACKEND builds where the postmaster needs to pass open file descriptors to the syslogger process via startup data rather than inheriting them through fork().

The function handles platform differences between Unix-like systems and Windows:
- On Unix/Linux: Uses the standard fileno() function to get the file descriptor
- On Windows: Uses _fileno() followed by _get_osfhandle() to get the OS file handle

This abstraction allows the same code to work across different operating systems while handling their different file descriptor/handle models.

## Parameters / Member Variables
- : FILE pointer to extract the file descriptor from (can be NULL)
- Returns: File descriptor/handle on success, -1 (Unix) or 0 (Windows) if file is NULL

## Dependencies
- Functions called/Symbols referenced:
  - fileno (Unix - gets file descriptor from FILE*)
  - _fileno/_get_osfhandle (Windows - gets OS handle from FILE*)
- Called from (representative examples):
  - [SysLogger_Start](../S/SysLogger_Start.md) (used three times to get descriptors for syslogFile, csvlogFile, jsonlogFile)

## Notes and Other Information
- This is a static function only used within the syslogger.c module
- Primarily used in EXEC_BACKEND builds where process creation doesn't use fork()
- Returns different sentinel values on different platforms (-1 on Unix, 0 on Windows) when file is NULL
- Part of the infrastructure that allows PostgreSQL to work on Windows where fork() is not available
- The file descriptors obtained are used to populate the SysloggerStartupData structure passed to the child logger process

## Simplified Source

```c
// Simplified version of syslogger_fdget
static int syslogger_fdget(FILE *file) {
    // Handle null file pointer - return platform-specific sentinel value
    if (file == NULL) {
#ifndef WIN32
        return -1;    // Unix: -1 indicates invalid descriptor
#else
        return 0;     // Windows: 0 indicates invalid handle
#endif
    }

    // Get file descriptor/handle from FILE pointer
#ifndef WIN32
    // Unix/Linux: Use standard fileno() function
    return fileno(file);
#else
    // Windows: Convert FILE* to OS handle via _fileno() and _get_osfhandle()
    return (int) _get_osfhandle(_fileno(file));
#endif
}
```

Key simplifications made:
- Restructured conditional logic for clearer flow
- Added explanatory comments for each platform-specific section
- Clarified the purpose of different return values
- Made the null pointer handling more explicit