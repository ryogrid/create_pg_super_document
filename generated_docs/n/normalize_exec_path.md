# normalize_exec_path

## Location
[src/common/exec.c:241-281](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/exec.c#L241-L281)

## Overview
Resolves symlinks and converts executable paths to absolute form, ensuring PostgreSQL utilities can reliably locate their true installation directory for finding related files.

## Definition

```c
static int
normalize_exec_path(char *path)
```
## Detailed Description
The  function performs path normalization on executable paths by:

1. Resolving all symbolic links to find the real file location
2. Converting the result to an absolute path
3. On Windows, ensuring path separators are converted from backslashes to forward slashes

This function is critical for PostgreSQL's installation layout because it ensures that programs can find their true installation directory, not just where a symlink might point. This is essential for locating related binaries, shared libraries, and data files that are installed relative to the executable's actual location.

The function leverages  to handle the complex work of symlink resolution and path absolutization, then performs platform-specific normalization as needed.

## Parameters / Member Variables
- : Input/output parameter containing the executable path to normalize (must be MAXPGPATH size)

## Dependencies
- Functions called/Symbols referenced:
  -  (resolves symlinks and converts to absolute path)
  -  (error logging)
  -  (safe string copying)
  -  (path canonicalization on Windows)
  -                total        used        free      shared  buff/cache   available
Mem:        32819380     6293256    22682248        3224     3843876    26143732
Swap:        8388608           0     8388608 (memory deallocation)
- Called from (representative examples):
  -  (multiple calls after finding valid executables)

## Notes and Other Information
- Returns 0 on success, -1 on failure
- Function is declared static (internal to exec.c)
- Modifies the input path in-place with the normalized result
- On Windows, ensures consistent forward slash path separators
- Critical for PostgreSQL's ability to locate installation-relative files
- Previously contained complex custom logic, now simplified to use pg_realpath()
- Handles memory management for the temporary absolute path returned by pg_realpath()

## Simplified Source

```c
// Simplified version of normalize_exec_path
static int normalize_exec_path(char *path) {
    // Step 1: Resolve symlinks and convert to absolute path
    char *absolute_path = pg_realpath(path);

    // Step 2: Handle failure case
    if (absolute_path == NULL) {
        log_error(errcode_for_file_access(),
                  "could not resolve path \"%s\" to absolute form: %m",
                  path);
        return -1;
    }

    // Step 3: Copy the resolved path back to input buffer
    strlcpy(path, absolute_path, MAXPGPATH);
    free(absolute_path);

    // Step 4: Platform-specific path normalization
    #ifdef WIN32
    canonicalize_path(path);  // Convert '\' to '/' on Windows
    #endif

    return 0;  // Success
}
```

Key simplifications made:
- Removed detailed comments about historical implementation changes
- Consolidated the core logic into clear sequential steps
- Simplified error handling while preserving essential functionality
- Added descriptive comments for each major operation
- Maintained the exact same algorithm and return behavior