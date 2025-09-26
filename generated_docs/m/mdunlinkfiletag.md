# mdunlinkfiletag

## Location
[src/backend/storage/smgr/md.c:1801-1819](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/md.c#L1801-L1819)

## Overview
Unlink (delete) a file identified by a file tag, providing the file path for error reporting purposes.

## Definition
```c
int mdunlinkfiletag(const FileTag *ftag, char *path)
```

## Detailed Description
This function removes a file from the filesystem based on a FileTag identifier. It constructs the permanent relation path using the relation locator from the file tag and the main fork, then attempts to unlink (delete) the file. The function is designed to be used when PostgreSQL needs to remove relation files, typically during DROP TABLE operations or when cleaning up after failed operations.

Unlike other file tag operations, this function specifically uses MAIN_FORKNUM and relpathperm(), suggesting it's intended for removing the main data files of relations rather than individual segments. The function provides the computed path to the caller for use in error messages and logging.

## Parameters / Member Variables
- `ftag`: Const pointer to FileTag structure containing the relation locator information to identify which file to unlink
- `path`: Output buffer (MAXPGPATH size) where the computed file path will be written for caller's use in error messages

## Dependencies
- Functions called/Symbols referenced:
  - relpathperm (to construct the permanent relation file path)
  - [strlcpy](../s/strlcpy.md) (for safe string copying into the output buffer)
  - [pfree](../p/pfree.md) (to free the allocated path string)
  - unlink (standard system call to remove the file)
- Called from (representative examples):
  - Used via MD_H header interface
  - Likely called during table drop operations or cleanup routines

## Notes and Other Information
- The function is part of the magnetic disk storage manager's public interface (declared in md.h)
- Always uses MAIN_FORKNUM, indicating it operates on main relation data files
- Returns standard Unix convention: 0 on success, -1 on failure with errno set
- Uses relpathperm() rather than segment-specific path construction, suggesting it works with entire relations
- Simple implementation compared to other file tag operations - no need to handle open file descriptors
- The path output parameter allows callers to report exactly which file failed to be unlinked
- Used for permanent file removal, not temporary operations