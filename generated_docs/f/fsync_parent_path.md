# fsync_parent_path

## Location
src/common/file_utils.c: 434 - 460

## Overview
A static function that synchronizes the parent directory of a given file or directory path to ensure filesystem metadata persistence.

## Definition


## Detailed Description
 extracts the parent directory path from the given filename and performs an fsync operation on it. This is crucial for ensuring that directory metadata changes (such as file creation, deletion, or renaming) are persistently written to disk, which is essential for crash recovery and ACID guarantees. The function handles the special case where the input is just a filename without a directory path by treating it as the current directory ("."). It uses  to extract the parent path and  to perform the actual synchronization.

## Parameters / Member Variables
- : The file or directory path whose parent directory should be synchronized
- : Error reporting level to use when logging errors (e.g., ERROR, WARNING, LOG)

## Dependencies
- Functions called/Symbols referenced:
  - strlcpy
  - get_parent_directory
  - fsync_fname_ext
- Called from (representative examples):
  - AllocateDesc
  - durable_rename
  - durable_unlink

## Notes and Other Information
This is a static function in the backend storage subsystem, indicating it's an internal implementation detail for file durability operations. There's also a public version in src/common/file_utils.c with a simpler interface (without elevel parameter) that's used by client-side utilities like pg_basebackup. The function is essential for ensuring that filesystem metadata operations survive system crashes, particularly important for database consistency during file operations like creating new database files or renaming existing ones.