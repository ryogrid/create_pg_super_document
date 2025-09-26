# do_truncate

## Location
src/backend/storage/smgr/md.c: 323 - 343

## Overview
A static helper function in the storage manager that truncates a file to zero length to release disk space, providing appropriate error handling and logging.

## Definition


## Detailed Description
The  function is a utility function within PostgreSQL's magnetic disk storage manager (md.c) that safely truncates a file to zero length. It serves as a wrapper around the system's  function, providing centralized error handling and logging functionality. The function is designed to handle cases where the file might not exist (ENOENT) gracefully while logging warnings for other types of errors. This function is primarily used during relation fork unlinking operations to ensure proper cleanup of storage files.

## Parameters / Member Variables
- : The file system path of the file to be truncated to zero length

## Dependencies
- Functions called/Symbols referenced:
  - pg_truncate
  - ereport (for warning logging)
  - errcode_for_file_access (for error code handling)
  - errmsg (for error message formatting)

- Called from (representative examples):
  - mdunlinkfork (multiple times during fork cleanup operations)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the md.c file
- Returns the result of  call (0 on success, -1 on failure)
- Specifically ignores ENOENT errors (file not found) without logging warnings, as this is considered a normal condition during cleanup
- Uses  pattern to preserve the original error code after logging
- The function provides a centralized point for file truncation error handling, avoiding code duplication in callers
- Part of PostgreSQL's storage management layer responsible for physical file operations