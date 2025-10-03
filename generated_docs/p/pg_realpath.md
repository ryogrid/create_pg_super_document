# pg_realpath

## Location
[src/common/exec.c:282-328](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/exec.c#L282-L328)

## Overview
PostgreSQL's cross-platform implementation of realpath() that resolves symlinks and returns absolute paths, providing POSIX.1-2008 semantics across different operating systems including Windows.

## Definition

```c
static char *
pg_realpath(const char *fname)
```
## Detailed Description
The  function is a portable wrapper around the system's path resolution functionality. It provides equivalent behavior to  on POSIX systems, returning a malloc'd buffer containing the absolute path equivalent to the input filename.

Key behaviors by platform:
- **Unix/Linux**: Uses standard  with fallback handling for older POSIX systems that require user-provided buffers
- **Windows**: Uses  as Microsoft's equivalent to POSIX realpath functionality
- **Legacy systems**: Handles old POSIX systems that don't support NULL buffer parameter by allocating a fixed-size buffer

The function handles error conditions appropriately:
- Returns NULL on error with errno set
- Manages memory carefully to avoid leaks on error paths
- On Windows, clears errno before calling  to ensure error detection

## Parameters / Member Variables
- `*fname`: The input filename/path to resolve to absolute form
## Dependencies
- Functions called/Symbols referenced:
  -  (POSIX path resolution function on Unix)
  -  (Windows path resolution function)
  -  (memory allocation for fallback buffer)
  -                total        used        free      shared  buff/cache   available
Mem:        32819380     6288788    22686672        3224     3843920    26148200
Swap:        8388608           0     8388608 (memory deallocation on error paths)
- Called from (representative examples):
  -  (primary caller for executable path resolution)

## Notes and Other Information
- Returns malloc'd buffer that caller must free
- Function is declared static (internal to exec.c)
- On Windows, result should typically be processed with  for consistent formatting
- Handles both modern POSIX systems and legacy systems with different realpath() behaviors
- Cross-platform abstraction allows consistent behavior across PostgreSQL supported platforms
- Critical for resolving symbolic links to find true executable locations
- Error handling preserves errno values for proper error reporting

## Simplified Source

```c
// Simplified version of pg_realpath
static char *pg_realpath(const char *fname) {
#ifndef WIN32
    // Try modern POSIX realpath first
    char *path = realpath(fname, NULL);

    // Handle old POSIX systems that need user-provided buffer
    if (path == NULL && errno == EINVAL) {
        char *buf = malloc(MAXPGPATH);
        if (buf == NULL) {
            return NULL;
        }

        path = realpath(fname, buf);
        if (path == NULL) {
            // Clean up on error
            int save_errno = errno;
            free(buf);
            errno = save_errno;
        }
    }
#else
    // Windows: use _fullpath instead of realpath
    errno = 0;
    char *path = _fullpath(NULL, fname, 0);
#endif

    return path;
}
```

Key simplifications made:
- Added clear platform-specific comments
- Streamlined the conditional compilation structure
- Preserved error handling and memory management
- Maintained cross-platform compatibility logic