# pg_check_dir

## Location
src/port/pgcheckdir.c: 33 - 92

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
  - `opendir` - Opens directory for reading
  - `readdir` - Reads directory entries
  - `closedir` - Closes directory handle
  - `DIR` - Directory handle type
  - `dirent` - Directory entry structure

- Called from (representative examples):
  - `create_data_directory` (initdb.c:2879)
  - `main` (initdb.c:3421)  
  - `verify_dir_is_empty_or_create` (pg_basebackup.c:749)
  - `bbsink_server_new` (basebackup_server.c:91)
  - `create_output_directory` (pg_combinebackup.c:720)
  - `cleanup_output_dirs` (pg_upgrade/util.c:79)
  - `create_fullpage_directory` (pg_waldump.c:132)

## Notes and Other Information
- The function is implemented in `src/port/pgcheckdir.c` and is part of PostgreSQL's portability layer
- On Windows systems, dot file detection and "lost+found" mount point detection are disabled via `#ifndef WIN32` preprocessor directives
- The function carefully manages errno to ensure that I/O errors during `readdir` operations are properly reported
- Directory closing errors will override previous success states and return -1
- The "." and ".." entries are always skipped as they are standard directory navigation entries
- This function is commonly used in PostgreSQL utilities that need to validate directory states before performing operations like database initialization, backups, or upgrades