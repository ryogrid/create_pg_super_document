# recurse_dir

## Location
src/bin/pg_rewind/file_ops.c: 374 - 468

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
  - opendir (directory operations)
  - readdir (directory entry reading) 
  - closedir (directory cleanup)
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