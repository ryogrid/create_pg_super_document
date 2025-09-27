# pg_file_exists

## Location
[src/backend/storage/file/fd.c:500-521](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L500-L521)

## Overview
A PostgreSQL utility function that checks whether a file exists at a given absolute path and ensures it is not a directory.

## Definition
```c
bool pg_file_exists(const char *name)
```

## Detailed Description
The pg_file_exists function determines if a file exists at the specified absolute path and verifies that it is indeed a file (not a directory). The function uses the stat() system call to obtain file information and checks the file mode to distinguish between files and directories. If the stat() call succeeds, the function returns true only if the path refers to a regular file or other non-directory entity. The function handles common error conditions gracefully - if the file doesn't exist (ENOENT), the path component doesn't exist (ENOTDIR), or access is denied (EACCES), it returns false. For other unexpected errors, it raises a PostgreSQL ERROR with appropriate error reporting.

## Parameters / Member Variables
- `name`: Absolute path to the file to check for existence (must not be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [stat](../s/stat.md) (system call)
  - S_ISDIR (macro)
  - Assert
  - ereport
  - [errcode_for_file_access](../e/errcode_for_file_access.md)
  - [errmsg](../e/errmsg.md)
- Called from (representative examples):
  - [provider_init](provider_init.md)
  - [expand_dynamic_library_name](../e/expand_dynamic_library_name.md)
  - [find_in_dynamic_libpath](../f/find_in_dynamic_libpath.md)
  - [injection_point_cache_load](../i/injection_point_cache_load.md)

## Notes and Other Information
- Returns true if the path exists and is not a directory
- Returns false if the file doesn't exist, path is invalid, access is denied, or path is a directory
- Requires an absolute path as input (asserted but not enforced programmatically)
- Raises ERROR for unexpected system call failures
- Part of PostgreSQL's file descriptor management utilities
- Commonly used in dynamic library loading and file discovery operations

## Simplified Source

```c
// Simplified version of pg_file_exists
bool pg_file_exists(const char *name) {
    struct stat st;

    // Validate input parameter
    Assert(name != NULL);

    // Check if file exists and get its attributes
    if (stat(name, &st) == 0) {
        // Return true only if it's not a directory
        return !S_ISDIR(st.st_mode);
    } else if (!(errno == ENOENT || errno == ENOTDIR || errno == EACCES)) {
        // Report unexpected errors (not file-not-found, path-invalid, or access-denied)
        ereport(ERROR, (errmsg("could not access file \"%s\": %m", name)));
    }

    // File doesn't exist or access denied
    return false;
}
```

Key simplifications made:
- Added clear comments explaining each step of the logic
- Preserved essential error handling for unexpected failures
- Maintained the directory vs file distinction using S_ISDIR
- Kept the graceful handling of common error conditions