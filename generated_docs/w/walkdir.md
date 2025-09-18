# walkdir

## Location
[src/common/file_utils.c:271-336](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/file_utils.c#L271-L336)

## Overview
Recursively traverses a directory tree, applying a specified action function to each regular file and directory encountered during the traversal.

## Definition


**Note**: This documentation covers the frontend version in . There is also a backend version in  with slightly different signature and error handling.

## Detailed Description
The  function provides recursive directory traversal functionality used by PostgreSQL's data synchronization utilities. It walks through a directory tree starting from the specified path and calls the provided action function for each file and directory encountered.

Key behavioral characteristics:
- Processes regular files and directories recursively
- Symbolic link handling is controlled by the  parameter, but symlinks are never followed in subdirectories
- The action function is called for both files () and directories ()
- Directory entries for "." and ".." are skipped
- The directory itself is processed after all its contents (post-order traversal for directories)
- Errors are logged but don't stop the traversal process

The function is critical for implementing file synchronization operations like fsync and pre-sync hinting across entire directory trees.

## Parameters / Member Variables
- : Starting directory path for the recursive traversal
- : Function pointer to call for each file/directory encountered; returns int and takes (filename, is_directory_flag) parameters
- : Boolean flag controlling whether to follow symbolic links in the top-level directory (always false for subdirectories)

## Dependencies
- Functions called/Symbols referenced:
  - opendir/readdir/closedir
  - [get_dirent_type](../g/get_dirent_type.md)
  - PGFILETYPE_REG
  - PGFILETYPE_DIR
  - [walkdir](walkdir.md) (recursive calls)
- Called from (representative examples):
  - [sync_pgdata](../s/sync_pgdata.md)
  - [sync_dir_recurse](../s/sync_dir_recurse.md)

## Notes and Other Information
- The function is declared , making it internal to file_utils.c
- There are two versions of walkdir: one in common/file_utils.c (frontend) and one in backend/storage/file/fd.c (backend)
- The frontend version uses standard POSIX directory functions while the backend version uses PostgreSQL's wrapped directory functions
- Directory processing follows post-order traversal - directory contents are processed before the directory itself
- This ensures proper fsync ordering where file entries are synced before directory entries
- The action function's return value is currently ignored in the common version
- Error handling is more lenient compared to the backend version - errors are logged but traversal continues