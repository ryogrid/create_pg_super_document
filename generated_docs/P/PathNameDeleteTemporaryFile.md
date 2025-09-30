# PathNameDeleteTemporaryFile

## Location
[src/backend/storage/file/fd.c:1929-1974](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L1929-L1974)

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
  - [stat](../s/stat.md) (system call to get file statistics)
  - unlink (system call to delete file)
  - [ReportTemporaryFileUsage](../R/ReportTemporaryFileUsage.md) (reports usage statistics)
  - ereport/ERROR/LOG (error reporting macros)

- Called from (representative examples):
  - [FileSetDelete](../F/FileSetDelete.md) (deletes files in file sets)
  - [unlink_if_exists_fname](../u/unlink_if_exists_fname.md) (general file deletion utility)

## Notes and Other Information
- Returns true if the file existed and was successfully deleted, false if it didn't exist
- The function specifically tolerates ENOENT (file not found) errors to support bulk deletion operations
- File size is captured before deletion attempt to ensure accurate usage reporting
- Usage statistics are only reported if the file was successfully stat'd before deletion
- Error reporting behavior is configurable: failures can be logged as ERROR or LOG level
- The stat-then-delete approach ensures that temporary file usage accounting remains accurate
- Designed to work with PostgreSQL's temporary file management and monitoring systems
- The tolerance for missing files makes it suitable for cleanup operations where files may have already been removed by other processes

## Simplified Source

```c
bool PathNameDeleteTemporaryFile(const char *path, bool error_on_failure)
{
    struct stat filestats;

    // Get file size for usage reporting
    bool file_exists = (stat(path, &filestats) == 0);

    // Tolerate non-existent files (return false = file didn't exist)
    if (!file_exists && errno == ENOENT)
        return false;

    // Try to delete the file
    if (unlink(path) < 0) {
        if (errno != ENOENT) {
            // Report error based on caller preference
            ereport(error_on_failure ? ERROR : LOG,
                    (errcode_for_file_access(),
                     errmsg("could not unlink temporary file \"%s\": %m", path)));
        }
        return false;
    }

    // Report usage statistics if we had valid file info
    if (file_exists) {
        ReportTemporaryFileUsage(path, filestats.st_size);
    } else {
        // Log stat failure but continue
        ereport(LOG, (errcode_for_file_access(),
                     errmsg("could not stat file \"%s\": %m", path)));
    }

    return true; // Successfully deleted
}
```