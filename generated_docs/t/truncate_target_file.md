# truncate_target_file

## Location
[src/bin/pg_rewind/file_ops.c:206-228](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/file_ops.c#L206-L228)

## Overview
Truncates a file in the target data directory to a specified size during PostgreSQL rewind operations.

## Definition
```c
void truncate_target_file(const char *path, off_t newsize)
```

## Detailed Description
This function is part of the pg_rewind utility's file operations module. It opens a specified file in the target PostgreSQL data directory and truncates it to the given size using the ftruncate() system call. The function handles the complete file operation cycle: opening the file with write permissions, performing the truncation, and properly closing the file descriptor. Like other file operations in pg_rewind, it respects the dry_run mode and includes comprehensive error handling.

## Parameters / Member Variables
- `path`: Relative path to the file within the target data directory that should be truncated
- `newsize`: The new size (in bytes) to which the file should be truncated

## Dependencies
- Functions called/Symbols referenced:
  - open (system call)
  - ftruncate (system call)
  - close (system call)
  - snprintf (standard library)
  - [pg_fatal](../p/pg_fatal.md) (PostgreSQL error handling)
- Called from (representative examples):
  - [perform_rewind](../p/perform_rewind.md) (src/bin/pg_rewind/pg_rewind.c:603)

## Notes and Other Information
- The function respects the global dry_run flag and performs no operations when dry_run is enabled
- Full target path is constructed by concatenating datadir_target with the provided relative path
- Opens files with O_WRONLY flag and pg_file_create_mode permissions
- Uses MAXPGPATH constant to ensure path buffer safety
- Provides detailed error messages for both open and ftruncate failures
- Part of the pg_rewind utility which synchronizes PostgreSQL data directories
- The newsize parameter is cast to unsigned int in error messages for consistent formatting

## Simplified Source

```c
void truncate_target_file(const char *path, off_t newsize) {
    char dstpath[MAXPGPATH];
    int fd;

    // Skip actual operation in dry run mode
    if (dry_run)
        return;

    // Build full target path
    snprintf(dstpath, sizeof(dstpath), "%s/%s", datadir_target, path);

    // Open file for writing
    fd = open(dstpath, O_WRONLY, pg_file_create_mode);
    if (fd < 0)
        pg_fatal("could not open file \"%s\" for truncation: %m", dstpath);

    // Truncate to specified size
    if (ftruncate(fd, newsize) != 0)
        pg_fatal("could not truncate file \"%s\" to %u: %m", dstpath, (unsigned int) newsize);

    close(fd);
}
```