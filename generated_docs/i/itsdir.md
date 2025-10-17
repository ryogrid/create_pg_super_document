# itsdir

## Location
[src/timezone/zic.c:1106-1130](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L1106-L1130)

## Overview
Determines whether a given path refers to a directory, using a robust approach that handles edge cases and systems without S_ISDIR macro support.

## Definition

```c
static bool
itsdir(char const *name)
```

## Detailed Description
The itsdir function checks if the specified path is a directory using a two-step approach for maximum compatibility:

1. **Primary method**: Uses the standard stat system call and S_ISDIR macro if available to check the file type directly from the st_mode field.

2. **Fallback method**: When S_ISDIR is unavailable, or when stat fails with specific conditions, it constructs a path ending with "/." and attempts to stat that path. Since "/." can only be successfully accessed if the original path is a directory, this serves as an effective directory test.

The function handles edge cases like EOVERFLOW errors (which can occur with very large file sizes) and provides compatibility for systems that don't support the S_ISDIR macro.

## Parameters / Member Variables
- `name`: The file system path to check for directory status

## Dependencies
- Functions called/Symbols referenced:
  - stat (POSIX system call for file information)
  - S_ISDIR (macro to test directory status, if available)
  - [emalloc](../e/emalloc.md) (memory allocation function)
  - strlen, memcpy, strcpy, free (standard C library functions)
- Called from:
  - [dolink](../d/dolink.md) (at line 1014 in src/timezone/zic.c)
  - [mkdirs](../m/mkdirs.md) (at line 3987 in src/timezone/zic.c)

## Notes and Other Information
- This is a static function local to src/timezone/zic.c, part of PostgreSQL's timezone handling code
- Returns true if the path is a directory, false otherwise
- Implements a robust fallback mechanism for systems lacking S_ISDIR support
- Handles EOVERFLOW errors gracefully, which can occur on 32-bit systems with very large files
- The fallback method cleverly uses the "/." suffix, as this can only be accessed successfully on directories
- Memory management is handled properly with emalloc and free for the temporary path construction

## Simplified Source

```c
static bool
itsdir(char const *name)
{
    struct stat st;
    int res = stat(name, &st);

    // Use S_ISDIR macro if available
#ifdef S_ISDIR
    if (res == 0)
        return S_ISDIR(st.st_mode) != 0;
#endif

    // Fallback method: try to stat "name/."
    if (res == 0 || errno == EOVERFLOW) {
        size_t n = strlen(name);
        char *nameslashdot = emalloc(n + 3);
        bool dir;

        memcpy(nameslashdot, name, n);
        // Add "/." or "." depending on whether name ends with "/"
        strcpy(&nameslashdot[n], &"/.\"[!(n && name[n - 1] != '/')]);
        dir = stat(nameslashdot, &st) == 0 || errno == EOVERFLOW;
        free(nameslashdot);
        return dir;
    }
    return false;
}
```