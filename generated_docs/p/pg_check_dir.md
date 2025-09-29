# pg_check_dir

## Location
[src/port/pgcheckdir.c:33-92](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/pgcheckdir.c#L33-L92)

## Overview
Tests whether a directory exists and determines its state (nonexistent, empty, contains only dot files, contains a mount point, or not empty).

## Definition
```c
int pg_check_dir(const char *dir)
```

## Detailed Description
The `pg_check_dir` function examines a directory path to determine its existence and content state. It opens the specified directory and iterates through its entries to classify the directory according to several predefined states. The function is particularly useful during PostgreSQL initialization and backup operations where directory state validation is critical.

The function uses standard POSIX directory operations (`opendir`, `readdir`, `closedir`) to examine directory contents. On non-Windows systems, it has special handling for dot files (files starting with ".") and the "lost+found" directory, which typically indicates a filesystem mount point.

The function carefully preserves errno values from `readdir` operations and handles directory closing errors appropriately to provide accurate error reporting to callers.

## Parameters / Member Variables
- `dir`: The path to the directory to be checked (const char pointer)

## Return Values
- `0`: Directory does not exist
- `1`: Directory exists and is empty
- `2`: Directory exists and contains only dot files (non-Windows only)
- `3`: Directory exists and contains a mount point (indicated by "lost+found" directory)
- `4`: Directory exists and is not empty (contains regular files/directories)
- `-1`: Error occurred while accessing directory (errno reflects the specific error)

## Dependencies
- Functions called/Symbols referenced:
  - `[opendir](../o/opendir.md)` - Opens directory for reading
  - `[readdir](../r/readdir.md)` - Reads directory entries
  - `[closedir](../c/closedir.md)` - Closes directory handle
  - `[DIR](../D/DIR.md)` - Directory handle type
  - `[dirent](../d/dirent.md)` - Directory entry structure

- Called from (representative examples):
  - `[create_data_directory](../c/create_data_directory.md)` (initdb.c:2879)
  - `[main](../m/main.md)` (initdb.c:3421)  
  - `[verify_dir_is_empty_or_create](../v/verify_dir_is_empty_or_create.md)` (pg_basebackup.c:749)
  - `[bbsink_server_new](../b/bbsink_server_new.md)` (basebackup_server.c:91)
  - `[create_output_directory](../c/create_output_directory.md)` (pg_combinebackup.c:720)
  - `[cleanup_output_dirs](../c/cleanup_output_dirs.md)` (pg_upgrade/util.c:79)
  - `[create_fullpage_directory](../c/create_fullpage_directory.md)` (pg_waldump.c:132)

## Notes and Other Information
- The function is implemented in `src/port/pgcheckdir.c` and is part of PostgreSQL's portability layer
- On Windows systems, dot file detection and "lost+found" mount point detection are disabled via `#ifndef WIN32` preprocessor directives
- The function carefully manages errno to ensure that I/O errors during `readdir` operations are properly reported
- Directory closing errors will override previous success states and return -1
- The "." and ".." entries are always skipped as they are standard directory navigation entries
- This function is commonly used in PostgreSQL utilities that need to validate directory states before performing operations like database initialization, backups, or upgrades

## Simplified Source

```c
int pg_check_dir(const char *dir)
{
    int result = 1;
    DIR *chkdir;
    struct dirent *file;
    bool dot_found = false;
    bool mount_found = false;
    int readdir_errno;

    // Try to open the directory
    chkdir = opendir(dir);
    if (chkdir == NULL)
        return (errno == ENOENT) ? 0 : -1;

    // Examine each directory entry
    while (errno = 0, (file = readdir(chkdir)) != NULL) {
        // Skip current and parent directory entries
        if (strcmp(".", file->d_name) == 0 || strcmp("..", file->d_name) == 0) {
            continue;
        }
#ifndef WIN32
        // Check for dot files (hidden files)
        else if (file->d_name[0] == '.') {
            dot_found = true;
        }
        // Check for lost+found directory (mount point indicator)
        else if (strcmp("lost+found", file->d_name) == 0) {
            mount_found = true;
        }
#endif
        else {
            // Found a regular file/directory - not empty
            result = 4;
            break;
        }
    }

    // Check for readdir errors
    if (errno)
        result = -1;

    // Close directory, preserving readdir errno on success
    readdir_errno = errno;
    if (closedir(chkdir))
        result = -1;
    else
        errno = readdir_errno;

    // Classify directory state based on findings
    if (result == 1 && mount_found)
        result = 3;  // Contains mount point
    if (result == 1 && dot_found)
        result = 2;  // Contains only dot files

    return result;
}
```