# do_truncate

## Location
[src/backend/storage/smgr/md.c:323-343](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/md.c#L323-L343)

## Overview
A static helper function in the storage manager that truncates a file to zero length to release disk space, providing appropriate error handling and logging.

## Definition

```c
static int
do_truncate(const char *path)
```
## Detailed Description
The  function is a utility function within PostgreSQL's magnetic disk storage manager (md.c) that safely truncates a file to zero length. It serves as a wrapper around the system's  function, providing centralized error handling and logging functionality. The function is designed to handle cases where the file might not exist (ENOENT) gracefully while logging warnings for other types of errors. This function is primarily used during relation fork unlinking operations to ensure proper cleanup of storage files.

## Parameters / Member Variables
- `*path`: The file system path of the file to be truncated to zero length
## Dependencies
- Functions called/Symbols referenced:
  - [pg_truncate](../p/pg_truncate.md)
  - ereport (for warning logging)
  - [errcode_for_file_access](../e/errcode_for_file_access.md) (for error code handling)
  - [errmsg](../e/errmsg.md) (for error message formatting)

- Called from (representative examples):
  - [mdunlinkfork](../m/mdunlinkfork.md) (multiple times during fork cleanup operations)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the md.c file
- Returns the result of  call (0 on success, -1 on failure)
- Specifically ignores ENOENT errors (file not found) without logging warnings, as this is considered a normal condition during cleanup
- Uses  pattern to preserve the original error code after logging
- The function provides a centralized point for file truncation error handling, avoiding code duplication in callers
- Part of PostgreSQL's storage management layer responsible for physical file operations

## Simplified Source

```c
static int do_truncate(const char *path) {
    // Attempt to truncate file to zero length
    int ret = pg_truncate(path, 0);

    // Log warning for errors (except file not found)
    if (ret < 0 && errno != ENOENT) {
        int save_errno = errno;
        ereport(WARNING, "could not truncate file \"%s\": %m", path);
        errno = save_errno; // Preserve original error
    }

    return ret; // 0 on success, -1 on failure
}
```