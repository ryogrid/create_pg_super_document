# destroy_tablespace_directories

## Location
[src/backend/commands/tablespace.c:686-852](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablespace.c#L686-L852)

## Overview
Removes the filesystem infrastructure of a tablespace by deleting database subdirectories, version directory, and symlink with different error handling for normal and WAL replay operations.

## Definition

```c
struct dirent *de;
```
## Detailed Description
destroy_tablespace_directories performs the physical removal of tablespace filesystem infrastructure, implementing a comprehensive cleanup process that handles both normal operations and WAL replay scenarios. The function systematically removes database subdirectories, the version directory, and the symlink while providing appropriate error handling based on the operational context.

The removal process follows a structured approach: first validating and removing individual database subdirectories, then removing the version directory, and finally handling symlink/directory removal. The function employs different error handling strategies for normal operations (strict ERROR reporting) versus WAL replay (permissive LOG reporting) to ensure database recoverability.

Special handling addresses potential race conditions during directory removal and accommodates the possibility of symlinks being replaced by directories during WAL replay scenarios. The function returns success/failure status to enable retry logic in calling functions.

## Parameters / Member Variables
- : OID of the tablespace whose directories should be destroyed
- : Boolean flag indicating WAL replay mode, which affects error handling severity

## Dependencies
- Functions called/Symbols referenced:
  - [DIR](../D/DIR.md), dirent: Directory handling structures and types
  - TABLESPACE_VERSION_DIRECTORY: Constant for version directory name
  - [AllocateDir](../A/AllocateDir.md): Opens directory for reading
  - [ReadDir](../R/ReadDir.md): Reads directory entries
  - [directory_is_empty](directory_is_empty.md): Checks if subdirectory contains files
  - [FreeDir](../F/FreeDir.md): Closes directory handle
  - [get_parent_directory](../g/get_parent_directory.md): Extracts parent directory path
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

## Simplified Source

```c
static bool destroy_tablespace_directories(Oid tablespaceoid, bool redo) {
    char *linkloc_with_version_dir;
    DIR *dirdesc;
    struct dirent *de;
    char *subfile;
    struct stat st;

    // Step 1: Build path to tablespace version directory
    linkloc_with_version_dir = psprintf("pg_tblspc/%u/%s", tablespaceoid,
                                       TABLESPACE_VERSION_DIRECTORY);

    // Step 2: Open tablespace directory for scanning
    dirdesc = AllocateDir(linkloc_with_version_dir);
    if (dirdesc == NULL) {
        if (errno == ENOENT) {
            // Directory doesn't exist - warn and proceed to symlink removal
            if (!redo) {
                ereport(WARNING, (errmsg("could not open directory \"%s\"",
                                        linkloc_with_version_dir)));
            }
            goto remove_symlink;
        } else if (redo) {
            // In WAL replay, log error and return failure
            ereport(LOG, (errmsg("could not open directory \"%s\"",
                                 linkloc_with_version_dir)));
            return false;
        }
        // Normal operation - let ReadDir report the error
    }

    // Step 3: Remove all database subdirectories
    while ((de = ReadDir(dirdesc, linkloc_with_version_dir)) != NULL) {
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
            continue;

        subfile = psprintf("%s/%s", linkloc_with_version_dir, de->d_name);

        // Check if subdirectory is empty before attempting removal
        if (!redo && !directory_is_empty(subfile)) {
            FreeDir(dirdesc);
            pfree(subfile);
            pfree(linkloc_with_version_dir);
            return false;  // Cannot remove non-empty directory
        }

        // Remove the empty database subdirectory
        if (rmdir(subfile) < 0) {
            ereport(redo ? LOG : ERROR, (errmsg("could not remove directory \"%s\"", subfile)));
        }

        pfree(subfile);
    }

    FreeDir(dirdesc);

    // Step 4: Remove the version directory itself
    if (rmdir(linkloc_with_version_dir) < 0) {
        ereport(redo ? LOG : ERROR, (errmsg("could not remove directory \"%s\"",
                                           linkloc_with_version_dir)));
        pfree(linkloc_with_version_dir);
        return false;
    }

remove_symlink:
    // Step 5: Remove the tablespace symlink or directory
    char *linkloc = pstrdup(linkloc_with_version_dir);
    get_parent_directory(linkloc);

    if (lstat(linkloc, &st) < 0) {
        int saved_errno = errno;
        ereport(redo ? LOG : (saved_errno == ENOENT ? WARNING : ERROR),
                (errmsg("could not stat file \"%s\"", linkloc)));
    } else if (S_ISDIR(st.st_mode)) {
        // Remove directory
        if (rmdir(linkloc) < 0) {
            ereport(redo ? LOG : (errno == ENOENT ? WARNING : ERROR),
                    (errmsg("could not remove directory \"%s\"", linkloc)));
        }
    } else if (S_ISLNK(st.st_mode)) {
        // Remove symlink
        if (unlink(linkloc) < 0) {
            ereport(redo ? LOG : (errno == ENOENT ? WARNING : ERROR),
                    (errmsg("could not remove symbolic link \"%s\"", linkloc)));
        }
    } else {
        // Neither directory nor symlink - error
        ereport(redo ? LOG : ERROR,
                (errmsg("\"%s\" is not a directory or symbolic link", linkloc)));
    }

    pfree(linkloc_with_version_dir);
    pfree(linkloc);

    return true;
}
```