# PathNameDeleteTemporaryDir

## Location
src/backend/storage/file/fd.c: 1688 - 1720

## Overview
PathNameDeleteTemporaryDir recursively deletes a temporary directory and all its contents, designed for cleanup operations in PostgreSQL's temporary file management system.

## Definition

```c
struct stat statbuf;
```
## Detailed Description
PathNameDeleteTemporaryDir performs a complete recursive deletion of a directory and all its contents, implementing a safe cleanup mechanism for PostgreSQL's temporary directories. The function silently handles the case where the directory doesn't exist, making it safe to call during cleanup operations regardless of whether the directory was actually created. It uses PostgreSQL's walkdir utility function with unlink_if_exists_fname callback to traverse and delete all files and subdirectories. Since this is primarily used in cleanup paths, the function logs failures rather than throwing errors, ensuring that cleanup operations continue even if some files cannot be deleted.

## Parameters / Member Variables
- : Path to the directory to be recursively deleted

## Dependencies
- Functions called/Symbols referenced:
  - stat
  - walkdir
  - unlink_if_exists_fname
- Called from (representative examples):
  - FileSetDeleteAll

## Notes and Other Information
This function is part of PostgreSQL's temporary file cleanup infrastructure in src/backend/storage/file/fd.c. It's designed to be robust in cleanup scenarios, silently ignoring missing directories and logging (rather than erroring on) deletion failures. The function uses the walkdir utility which traverses the directory tree and applies the unlink_if_exists_fname callback to each entry. The implementation prioritizes cleanup completion over strict error handling, which is appropriate for its use in resource cleanup scenarios. The function currently has a limitation in that walkdir doesn't provide state management for error reporting, though this is acceptable given its cleanup-focused usage pattern.