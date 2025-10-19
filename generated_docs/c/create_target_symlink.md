# create_target_symlink

## Location
[src/bin/pg_rewind/file_ops.c:257-270](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/file_ops.c#L257-L270)

## Overview
Creates a symbolic link in the target data directory during PostgreSQL rewind operations.

## Definition
```c
static void create_target_symlink(const char *path, const char *link)
```

## Detailed Description
This is a static helper function within the pg_rewind utility's file operations module. It creates a symbolic link at the specified path within the target PostgreSQL data directory using the symlink() system call. The function constructs the full target path where the symlink should be created and creates it pointing to the specified link target. As with other file operations in pg_rewind, it respects the dry_run mode and includes comprehensive error handling for symlink creation failures.

## Parameters / Member Variables
- `path`: Relative path within the target data directory where the symbolic link should be created
- `link`: The target path or content that the symbolic link should point to

## Dependencies
- Functions called/Symbols referenced:
  - symlink (system call)
  - snprintf (standard library)
  - [pg_fatal](../p/pg_fatal.md) (PostgreSQL error handling)
- Called from (representative examples):
  - [create_target](create_target.md) (src/bin/pg_rewind/file_ops.c:168)

## Notes and Other Information
- This is a static function, only accessible within the file_ops.c module
- The function respects the global dry_run flag and performs no operations when dry_run is enabled
- Full target path is constructed by concatenating datadir_target with the provided relative path
- Uses MAXPGPATH constant to ensure path buffer safety
- The link parameter can be either an absolute path or a relative path, depending on the desired symlink behavior
- Provides detailed error messages including the full path where the symlink creation failed
- Part of the pg_rewind utility which synchronizes PostgreSQL data directories
- Symbolic links are commonly used in PostgreSQL installations for tablespaces and other external storage locations
- Function assumes the parent directory already exists and will fail if the parent path is missing

## Simplified Source

```c
static void create_target_symlink(const char *path, const char *link)
{
    char dstpath[MAXPGPATH];

    // Skip operation in dry run mode
    if (dry_run)
        return;

    // Build full target path and create symlink
    snprintf(dstpath, sizeof(dstpath), "%s/%s", datadir_target, path);
    if (symlink(link, dstpath) != 0)
        pg_fatal("could not create symbolic link at \"%s\": %m", dstpath);
}
```