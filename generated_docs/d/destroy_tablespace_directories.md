# destroy_tablespace_directories

## Location
src/backend/commands/tablespace.c: 686 - 852

## Overview
Removes the filesystem infrastructure of a tablespace by deleting database subdirectories, version directory, and symlink with different error handling for normal and WAL replay operations.

## Definition


## Detailed Description
destroy_tablespace_directories performs the physical removal of tablespace filesystem infrastructure, implementing a comprehensive cleanup process that handles both normal operations and WAL replay scenarios. The function systematically removes database subdirectories, the version directory, and the symlink while providing appropriate error handling based on the operational context.

The removal process follows a structured approach: first validating and removing individual database subdirectories, then removing the version directory, and finally handling symlink/directory removal. The function employs different error handling strategies for normal operations (strict ERROR reporting) versus WAL replay (permissive LOG reporting) to ensure database recoverability.

Special handling addresses potential race conditions during directory removal and accommodates the possibility of symlinks being replaced by directories during WAL replay scenarios. The function returns success/failure status to enable retry logic in calling functions.

## Parameters / Member Variables
- : OID of the tablespace whose directories should be destroyed
- : Boolean flag indicating WAL replay mode, which affects error handling severity

## Dependencies
- Functions called/Symbols referenced:
  - DIR, dirent: Directory handling structures and types
  - TABLESPACE_VERSION_DIRECTORY: Constant for version directory name
  - AllocateDir: Opens directory for reading
  - ReadDir: Reads directory entries
  - [directory_is_empty](directory_is_empty.md): Checks if subdirectory contains files
  - FreeDir: Closes directory handle
  - get_parent_directory: Extracts parent directory path
  - lstat: Gets file/symlink status without following links
  - S_ISDIR, S_ISLNK: File type checking macros
  - unlink: Removes files and symlinks
- Called from (representative examples):
  - [DropTableSpace](../D/DropTableSpace.md): During tablespace deletion (with retry logic)
  - [tblspc_redo](../t/tblspc_redo.md): During WAL replay for tablespace operations

## Notes and Other Information
- Returns boolean indicating success (true) or failure due to non-empty directories (false)
- Uses different error reporting levels based on redo flag: ERROR for normal operations, LOG for WAL replay
- Handles missing directories gracefully with warnings rather than errors
- Implements comprehensive cleanup of database subdirectories before attempting version directory removal
- Accommodates both symlink and directory removal for the tablespace link
- Provides detailed error messages distinguishing between different failure modes
- Designed to be retryable - partial failures don't leave inconsistent state
- Protected by TablespaceCreateLock held by caller to prevent concurrent modifications