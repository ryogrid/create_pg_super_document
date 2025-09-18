# unlink_initfile

## Location
src/backend/utils/cache/relcache.c: 6863 - 6879

## Overview
Low-level helper function that safely removes a relation cache initialization file with appropriate error handling and logging.

## Definition
```c
static void unlink_initfile(const char *initfilename, int elevel)
```

## Detailed Description
This static utility function provides a safe wrapper around the system unlink() call for removing relation cache initialization files. It implements proper error handling by distinguishing between expected (ENOENT - file not found) and unexpected errors.

The function's behavior:
1. Attempts to remove the specified file using unlink()
2. If the removal fails and the error is NOT ENOENT (file not found), it reports the error at the specified error level
3. ENOENT errors are silently ignored since the file might legitimately not exist
4. Other errors (permissions, I/O errors, etc.) are reported with a descriptive error message

This approach allows callers to attempt file removal without worrying about whether the file exists, while still being notified of genuine problems that might indicate system issues.

## Parameters / Member Variables
- `initfilename`: The full path to the initialization file to be removed
- `elevel`: The error reporting level to use if an unexpected error occurs (typically LOG)

## Dependencies
- Functions called/Symbols referenced:
  - unlink (system call)
  - ereport (for error reporting)
  - [errcode_for_file_access](../e/errcode_for_file_access.md) (for error codes)
  - [errmsg](../e/errmsg.md) (for error messages)
- Called from (representative examples):
  - [RelationCacheInitFilePreInvalidate](../R/RelationCacheInitFilePreInvalidate.md)
  - [RelationCacheInitFileRemove](../R/RelationCacheInitFileRemove.md)  
  - [RelationCacheInitFileRemoveInDir](../R/RelationCacheInitFileRemoveInDir.md)

## Notes and Other Information
- This is a static function, only accessible within relcache.c
- The function treats ENOENT as a normal condition, not an error
- Uses PostgreSQL's standard error reporting mechanism with proper error codes
- The error level parameter allows callers to control how aggressively errors are reported
- Typically called with LOG level, meaning errors are logged but don't abort the operation