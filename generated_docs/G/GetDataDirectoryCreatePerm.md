# GetDataDirectoryCreatePerm

## Location
[src/common/file_perm.c:66-87](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/file_perm.c#L66-L87)

## Overview
Examines the permissions of a PostgreSQL data directory and automatically configures global file creation permissions to match, providing a convenient wrapper around SetDataDirectoryCreatePerm.

## Definition
```c
bool GetDataDirectoryCreatePerm(const char *dataDir)
```

## Detailed Description
This frontend-only function retrieves the file system permissions of the specified data directory using stat() and automatically calls SetDataDirectoryCreatePerm() to configure PostgreSQL's global permission variables accordingly. The function provides error handling by returning false if the directory cannot be accessed, allowing callers to handle permission setup failures gracefully. On Windows and Cygwin platforms, the permission setting is skipped due to limited Unix-style permission support, but stat() is still performed for consistent behavior.

## Parameters / Member Variables
- `dataDir`: Path to the PostgreSQL data directory whose permissions should be examined and applied

## Dependencies
- Functions called/Symbols referenced:
  - [stat](../s/stat.md)() (system call for getting file status)
  - [SetDataDirectoryCreatePerm](../S/SetDataDirectoryCreatePerm.md)() (to configure permission globals)
- Called from (representative examples):
  - [main](../m/main.md) (src/bin/pg_ctl/pg_ctl.c:2455)
  - [main](../m/main.md) (src/bin/pg_resetwal/pg_resetwal.c:350)
  - [main](../m/main.md) (src/bin/pg_rewind/pg_rewind.c:292)
  - [main](../m/main.md) (src/bin/pg_upgrade/pg_upgrade.c:114)

## Notes and Other Information
- This function is only available in frontend applications (compiled with FRONTEND defined)
- Returns true on success, false if stat() fails on the directory
- Error handling is delegated to the caller - the function does not generate error messages
- On non-Windows platforms, automatically configures permission globals; on Windows, only validates directory accessibility
- Commonly used by PostgreSQL utilities that need to operate with the same permissions as the data directory
- Essential for maintaining consistent file permissions across PostgreSQL's various command-line tools