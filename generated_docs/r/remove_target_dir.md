# remove_target_dir

## Location
[src/bin/pg_rewind/file_ops.c:243-256](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/file_ops.c#L243-L256)

## Overview
Removes an empty directory from the target data directory during PostgreSQL rewind operations.

## Definition
```c
static void remove_target_dir(const char *path)
```

## Detailed Description
This is a static helper function within the pg_rewind utility's file operations module. It removes an empty directory at the specified path within the target PostgreSQL data directory using the rmdir() system call. The function constructs the full target path and attempts to remove the directory. As with other file operations in pg_rewind, it respects the dry_run mode and includes comprehensive error handling for directory removal failures. Note that rmdir() will only succeed if the directory is empty.

## Parameters / Member Variables
- `path`: Relative path within the target data directory of the directory that should be removed

## Dependencies
- Functions called/Symbols referenced:
  - rmdir (system call)
  - snprintf (standard library)
  - [pg_fatal](../p/pg_fatal.md) (PostgreSQL error handling)
- Called from (representative examples):
  - [remove_target](remove_target.md) (src/bin/pg_rewind/file_ops.c:138)

## Notes and Other Information
- This is a static function, only accessible within the file_ops.c module
- The function respects the global dry_run flag and performs no operations when dry_run is enabled
- Full target path is constructed by concatenating datadir_target with the provided relative path
- Uses MAXPGPATH constant to ensure path buffer safety
- The rmdir() system call will only succeed if the directory is empty; non-empty directories will cause the operation to fail
- Provides detailed error messages including the full path and system error details
- Part of the pg_rewind utility which synchronizes PostgreSQL data directories
- Unlike remove_target_file, this function does not have a missing_ok parameter and will always report errors if the directory removal fails

## Simplified Source

```c
static void remove_target_dir(const char *path) {
    char dstpath[MAXPGPATH];

    // Skip actual operation in dry run mode
    if (dry_run)
        return;

    // Build full target path
    snprintf(dstpath, sizeof(dstpath), "%s/%s", datadir_target, path);

    // Remove directory (must be empty)
    if (rmdir(dstpath) != 0)
        pg_fatal("could not remove directory \"%s\": %m", dstpath);
}
```