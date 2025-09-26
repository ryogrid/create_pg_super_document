# PathNameDeleteTemporaryFile

## Location
src/backend/storage/file/fd.c: 1929 - 1974

## Overview
PathNameDeleteTemporaryFile deletes a temporary file by pathname and reports its usage statistics, with graceful handling of non-existent files.

## Definition

```c
struct stat filestats;
```
## Detailed Description
This function deletes a temporary file at the specified path while handling various error conditions gracefully and reporting usage statistics for monitoring purposes. The function is designed to be robust in scenarios where files might already be deleted or might not exist.

The function performs these operations:
1. Stats the file to get its size before deletion (for usage reporting)
2. Attempts to unlink (delete) the file
3. Reports temporary file usage statistics if successful
4. Handles various error conditions with configurable error reporting

Unlike automatic file deletion in FileClose, this function tolerates non-existence to support use cases like BufFileDeleteFileSet, which doesn't know in advance how many file segments exist.

## Parameters / Member Variables
- : Full filesystem path to the temporary file to delete
- : If true, reports unlink failures as ERROR; if false, reports as LOG

## Dependencies
- Functions called/Symbols referenced:
  - stat (system call to get file statistics)
  - unlink (system call to delete file)
  - ReportTemporaryFileUsage (reports usage statistics)
  - ereport/ERROR/LOG (error reporting macros)

- Called from (representative examples):
  - FileSetDelete (deletes files in file sets)
  - unlink_if_exists_fname (general file deletion utility)

## Notes and Other Information
- Returns true if the file existed and was successfully deleted, false if it didn't exist
- The function specifically tolerates ENOENT (file not found) errors to support bulk deletion operations
- File size is captured before deletion attempt to ensure accurate usage reporting
- Usage statistics are only reported if the file was successfully stat'd before deletion
- Error reporting behavior is configurable: failures can be logged as ERROR or LOG level
- The stat-then-delete approach ensures that temporary file usage accounting remains accurate
- Designed to work with PostgreSQL's temporary file management and monitoring systems
- The tolerance for missing files makes it suitable for cleanup operations where files may have already been removed by other processes