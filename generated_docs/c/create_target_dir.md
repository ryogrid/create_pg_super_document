# create_target_dir

## Location
[src/bin/pg_rewind/file_ops.c:229-242](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/file_ops.c#L229-L242)

## Overview
Creates a directory in the target data directory during PostgreSQL rewind operations.

## Definition
```c
static void create_target_dir(const char *path)
```

## Detailed Description
This is a static helper function within the pg_rewind utility's file operations module. It creates a new directory at the specified path within the target PostgreSQL data directory using the mkdir() system call. The function constructs the full target path and creates the directory with appropriate PostgreSQL directory permissions. As with other file operations in pg_rewind, it respects the dry_run mode and includes error handling for directory creation failures.

## Parameters / Member Variables
- `path`: Relative path within the target data directory where the new directory should be created

## Dependencies
- Functions called/Symbols referenced:
  - mkdir (system call)
  - snprintf (standard library)
  - [pg_fatal](../p/pg_fatal.md) (PostgreSQL error handling)
- Called from (representative examples):
  - [create_target](create_target.md) (src/bin/pg_rewind/file_ops.c:164)

## Notes and Other Information
- This is a static function, only accessible within the file_ops.c module
- The function respects the global dry_run flag and performs no operations when dry_run is enabled
- Full target path is constructed by concatenating datadir_target with the provided relative path
- Uses pg_dir_create_mode for directory permissions, ensuring consistent PostgreSQL directory permissions
- Uses MAXPGPATH constant to ensure path buffer safety
- Provides detailed error messages including the full path and system error details
- Part of the pg_rewind utility which synchronizes PostgreSQL data directories
- Function assumes the parent directory already exists and will fail if the parent path is missing