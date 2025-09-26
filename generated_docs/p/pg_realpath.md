# pg_realpath

## Location
src/common/exec.c: 282 - 328

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
- : The input filename/path to resolve to absolute form

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