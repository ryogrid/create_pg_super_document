# get_dirent_type

## Location
src/common/file_utils.c: 525 - 591

## Overview
A cross-platform function that determines the file type of a directory entry, with fallback mechanisms for systems that don't provide type information directly.

## Definition


## Detailed Description
 provides a portable way to determine whether a directory entry represents a regular file, directory, symbolic link, or other file type. The function first attempts to use the BSD/Linux extension  field from the dirent structure for efficiency. If this information is unavailable or unknown (common on some filesystems), it falls back to using  or  system calls to determine the file type. The function can optionally follow symbolic links or treat them as links depending on the  parameter. Error handling is unified for both frontend and backend code through conditional compilation.

## Parameters / Member Variables
- : Full path to the file/directory entry being examined
- : Pointer to the dirent structure from readdir() containing the directory entry
- : If true, follows symbolic links to determine the target's type; if false, returns PGFILETYPE_LNK for symbolic links
- : Error reporting level for logging failures (frontend: logging.h levels, backend: elog.h levels)

## Dependencies
- Functions called/Symbols referenced:
  - dirent
  - PGFileType
  - DT_REG, DT_DIR, DT_LNK (BSD/Linux dirent type constants)
  - stat, lstat (system calls)
  - S_ISREG, S_ISDIR, S_ISLNK (POSIX stat macros)
  - pg_log_generic (frontend logging)
  - ereport (backend logging)
- Called from (representative examples):
  - CheckPointLogicalRewriteHeap
  - RemoveXlogFile
  - copydir
  - walkdir
  - rmtree
  - process_directory_recursively

## Notes and Other Information
This function is part of PostgreSQL's common utilities and works in both frontend tools and backend code. It abstracts away platform differences in directory entry type detection, providing a consistent interface across different operating systems. The function returns PGFileType enum values: PGFILETYPE_REG (regular file), PGFILETYPE_DIR (directory), PGFILETYPE_LNK (symbolic link), PGFILETYPE_UNKNOWN (unknown type), or PGFILETYPE_ERROR (stat failed). This is essential for directory traversal operations in PostgreSQL utilities and server-side file management.