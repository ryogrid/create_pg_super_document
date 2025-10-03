# get_dirent_type

## Location
[src/common/file_utils.c:525-591](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/file_utils.c#L525-L591)

## Overview
A cross-platform function that determines the file type of a directory entry, with fallback mechanisms for systems that don't provide type information directly.

## Definition

```c
PGFileType
get_dirent_type(const char *path,
				const struct dirent *de,
				bool look_through_symlinks,
				int elevel)
```
## Detailed Description
 provides a portable way to determine whether a directory entry represents a regular file, directory, symbolic link, or other file type. The function first attempts to use the BSD/Linux extension  field from the dirent structure for efficiency. If this information is unavailable or unknown (common on some filesystems), it falls back to using  or  system calls to determine the file type. The function can optionally follow symbolic links or treat them as links depending on the  parameter. Error handling is unified for both frontend and backend code through conditional compilation.

## Parameters / Member Variables
- `*path`: Full path to the file/directory entry being examined
- `*de`: Pointer to the dirent structure from readdir() containing the directory entry
- `look_through_symlinks`: If true, follows symbolic links to determine the target's type; if false, returns PGFILETYPE_LNK for symbolic links
- `elevel`: Error reporting level for logging failures (frontend: logging.h levels, backend: elog.h levels)
## Dependencies
- Functions called/Symbols referenced:
  - [dirent](../d/dirent.md)
  - [PGFileType](../P/PGFileType.md)
  - DT_REG, DT_DIR, DT_LNK (BSD/Linux dirent type constants)
  - [stat](../s/stat.md), lstat (system calls)
  - S_ISREG, S_ISDIR, S_ISLNK (POSIX stat macros)
  - [pg_log_generic](../p/pg_log_generic.md) (frontend logging)
  - ereport (backend logging)
- Called from (representative examples):
  - [CheckPointLogicalRewriteHeap](../C/CheckPointLogicalRewriteHeap.md)
  - [RemoveXlogFile](../R/RemoveXlogFile.md)
  - [copydir](../c/copydir.md)
  - [walkdir](../w/walkdir.md)
  - [rmtree](../r/rmtree.md)
  - [process_directory_recursively](../p/process_directory_recursively.md)

## Notes and Other Information
This function is part of PostgreSQL's common utilities and works in both frontend tools and backend code. It abstracts away platform differences in directory entry type detection, providing a consistent interface across different operating systems. The function returns PGFileType enum values: PGFILETYPE_REG (regular file), PGFILETYPE_DIR (directory), PGFILETYPE_LNK (symbolic link), PGFILETYPE_UNKNOWN (unknown type), or PGFILETYPE_ERROR (stat failed). This is essential for directory traversal operations in PostgreSQL utilities and server-side file management.

## Simplified Source

```c
// Simplified version of get_dirent_type
PGFileType get_dirent_type(const char *path,
                          const struct dirent *de,
                          bool look_through_symlinks,
                          int elevel) {
    PGFileType result;

    // Step 1: Try to get type from dirent structure (fast path on BSD/Linux)
    if (de->d_type == DT_REG)
        result = PGFILETYPE_REG;
    else if (de->d_type == DT_DIR)
        result = PGFILETYPE_DIR;
    else if (de->d_type == DT_LNK && !look_through_symlinks)
        result = PGFILETYPE_LNK;
    else
        result = PGFILETYPE_UNKNOWN;

    // Step 2: If type unknown, use stat() as fallback
    if (result == PGFILETYPE_UNKNOWN) {
        struct stat file_stat;
        int stat_result;

        // Choose stat vs lstat based on symlink handling preference
        if (look_through_symlinks)
            stat_result = stat(path, &file_stat);
        else
            stat_result = lstat(path, &file_stat);

        // Step 3: Handle stat results
        if (stat_result < 0) {
            result = PGFILETYPE_ERROR;
            // Log error (implementation varies by frontend/backend)
        } else {
            // Determine file type from stat mode
            if (S_ISREG(file_stat.st_mode))
                result = PGFILETYPE_REG;
            else if (S_ISDIR(file_stat.st_mode))
                result = PGFILETYPE_DIR;
            else if (S_ISLNK(file_stat.st_mode))
                result = PGFILETYPE_LNK;
        }
    }

    return result;
}
```

Key simplifications made:
- Removed platform-specific conditional compilation directives
- Consolidated error logging into a single comment (actual implementation varies)
- Used more descriptive variable names (file_stat instead of fst)
- Added step-by-step comments explaining the logic flow
- Focused on the main algorithm: try dirent first, fallback to stat
- Abstracted the frontend/backend logging differences