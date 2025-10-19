# recurse_dir

## Location
[src/bin/pg_rewind/file_ops.c:374-468](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/file_ops.c#L374-L468)

## Overview
Recursively traverses directory structures, calling a callback function for each file, directory, and symlink encountered while handling special PostgreSQL directory cases.

## Definition
```c
static void recurse_dir(const char *datadir, const char *parentpath, process_file_callback_t callback)
```

## Detailed Description
This function implements the core recursive directory traversal logic for PostgreSQL data directory processing. It opens and reads directories, processes each entry according to its type (regular file, directory, or symlink), and recursively descends into subdirectories. The function includes special handling for PostgreSQL-specific directories like pg_tblspc (tablespaces) and pg_wal, where symlinks are followed for complete traversal. It also handles edge cases such as files that disappear during traversal (common in active databases) and provides comprehensive error handling for all filesystem operations.

## Parameters / Member Variables
- `datadir`: Root data directory path for the traversal
- `parentpath`: Current subdirectory path relative to datadir (NULL for top level)
- `callback`: Function pointer of type process_file_callback_t called for each file system entry

## Dependencies
- Functions called/Symbols referenced:
  - [opendir](../o/opendir.md) (directory operations)
  - [readdir](readdir.md) (directory entry reading) 
  - [closedir](../c/closedir.md) (directory cleanup)
  - lstat (file status information)
  - readlink (symlink target reading)
  - S_ISREG, S_ISDIR, S_ISLNK (file type macros)
  - FILE_TYPE_REGULAR, FILE_TYPE_DIRECTORY, FILE_TYPE_SYMLINK (constants)
  - snprintf (path construction)
  - [pg_fatal](../p/pg_fatal.md) (error reporting)
- Called from (representative examples):
  - [traverse_datadir](../t/traverse_datadir.md) (file_ops.c:364)
  - [recurse_dir](recurse_dir.md) (self-recursion at lines 432, 457)

## Notes and Other Information
- This is a static function, only accessible within file_ops.c
- Uses MAXPGPATH and MAXPGPATH*2 for safe path buffer sizing
- Gracefully handles ENOENT errors (files disappearing during traversal) which can occur in active database environments
- Special symlink handling: follows symlinks in pg_tblspc directory and for pg_wal to ensure complete tablespace and WAL directory processing
- Skips '.' and '..' directory entries to prevent infinite recursion
- Provides detailed error messages for all failure cases including directory open/read/close and file stat operations
- The callback function receives different parameters based on file type: size for regular files, link target for symlinks
- Critical component of pg_rewind's file comparison and synchronization operations

## Simplified Source

```c
static void recurse_dir(const char *datadir, const char *parentpath,
                       process_file_callback_t callback)
{
    DIR *directory;
    struct dirent *entry;
    char full_directory_path[MAXPGPATH];

    // Build full path to current directory
    if (parentpath)
        snprintf(full_directory_path, MAXPGPATH, "%s/%s", datadir, parentpath);
    else
        snprintf(full_directory_path, MAXPGPATH, "%s", datadir);

    // Open directory for reading
    directory = opendir(full_directory_path);
    if (directory == NULL)
        pg_fatal("could not open directory \"%s\": %m", full_directory_path);

    // Process each entry in the directory
    while ((entry = readdir(directory)) != NULL)
    {
        struct stat file_stats;
        char full_file_path[MAXPGPATH * 2];
        char relative_path[MAXPGPATH * 2];

        // Skip current and parent directory entries
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
            continue;

        // Build full path to file/directory
        snprintf(full_file_path, sizeof(full_file_path), "%s/%s",
                full_directory_path, entry->d_name);

        // Get file statistics - handle disappearing files gracefully
        if (lstat(full_file_path, &file_stats) < 0) {
            if (errno == ENOENT)
                continue; // File disappeared, skip it
            else
                pg_fatal("could not stat file \"%s\": %m", full_file_path);
        }

        // Build relative path for callback
        if (parentpath)
            snprintf(relative_path, sizeof(relative_path), "%s/%s",
                    parentpath, entry->d_name);
        else
            snprintf(relative_path, sizeof(relative_path), "%s", entry->d_name);

        // Handle different file types
        if (S_ISREG(file_stats.st_mode)) {
            // Regular file
            callback(relative_path, FILE_TYPE_REGULAR, file_stats.st_size, NULL);
        }
        else if (S_ISDIR(file_stats.st_mode)) {
            // Directory - call callback then recurse
            callback(relative_path, FILE_TYPE_DIRECTORY, 0, NULL);
            recurse_dir(datadir, relative_path, callback);
        }
        else if (S_ISLNK(file_stats.st_mode)) {
            // Symbolic link - read target and optionally recurse
            char link_target[MAXPGPATH];
            int target_length = readlink(full_file_path, link_target, sizeof(link_target));

            if (target_length < 0)
                pg_fatal("could not read symbolic link \"%s\": %m", full_file_path);
            if (target_length >= sizeof(link_target))
                pg_fatal("symbolic link \"%s\" target is too long", full_file_path);

            link_target[target_length] = '\0';
            callback(relative_path, FILE_TYPE_SYMLINK, 0, link_target);

            // Recurse into tablespace and WAL symlinks
            if ((parentpath && strcmp(parentpath, "pg_tblspc") == 0) ||
                strcmp(relative_path, "pg_wal") == 0)
                recurse_dir(datadir, relative_path, callback);
        }
    }

    // Check for directory reading errors and close
    if (errno)
        pg_fatal("could not read directory \"%s\": %m", full_directory_path);
    if (closedir(directory))
        pg_fatal("could not close directory \"%s\": %m", full_directory_path);
}
```