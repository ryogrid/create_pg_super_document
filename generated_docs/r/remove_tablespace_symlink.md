# remove_tablespace_symlink

## Location
[src/backend/commands/tablespace.c:883-929](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablespace.c#L883-L929)

## Overview
Removes tablespace symlinks or directories from the pg_tblspc directory, handling both Unix symbolic links and Windows junction points with proper error handling.

## Definition
void remove_tablespace_symlink(const char *linkloc)

## Detailed Description
This function safely removes tablespace links from the PostgreSQL pg_tblspc directory. It handles platform-specific differences where Unix systems use symbolic links while Windows uses junction points (which appear as directories). The function first checks if the target exists using lstat(), then determines the appropriate removal method based on the file type. For directories (including Windows junction points), it uses rmdir(); for symbolic links, it uses unlink(). The function treats non-existent files as success but reports errors for any other failure conditions or unexpected file types.

## Parameters / Member Variables
- linkloc: The filesystem path to the tablespace symlink or junction point to be removed

## Dependencies
- Functions called/Symbols referenced:
  - lstat: Gets file status information without following symbolic links
  - S_ISDIR: Macro to test if file status indicates a directory
  - S_ISLNK: Macro to test if file status indicates a symbolic link  
  - unlink: Removes a symbolic link or file
  - rmdir: Removes an empty directory
  - ereport: PostgreSQL error reporting function
  - [errcode_for_file_access](../e/errcode_for_file_access.md): Error code function for file access errors

- Called from (representative examples):
  - [InitWalRecovery](../I/InitWalRecovery.md): Used during WAL recovery initialization
  - [create_tablespace_directories](../c/create_tablespace_directories.md): Part of tablespace creation process

## Notes and Other Information
- Designed to handle cross-platform differences between Unix symlinks and Windows junction points
- Non-existent files are treated as successful removal (ENOENT is not an error)
- Will fail if attempting to remove a directory that is not empty (except for junction points)
- Refuses to remove files that are neither directories nor symbolic links as a safety measure
- Always reports errors for removal failures, unlike some similar functions that may ignore certain error conditions
- Critical for maintaining consistency in the pg_tblspc directory structure during tablespace operations

## Simplified Source

```c
// Simplified version of remove_tablespace_symlink
void remove_tablespace_symlink(const char *linkloc) {
    struct stat st;

    // Step 1: Check if the file exists
    if (lstat(linkloc, &st) < 0) {
        if (errno == ENOENT)
            return; // File doesn't exist - that's OK
        ereport(ERROR, (errcode_for_file_access(),
                errmsg("could not stat file \"%s\": %m", linkloc)));
    }

    // Step 2: Remove based on file type
    if (S_ISDIR(st.st_mode)) {
        // Directory (including Windows junction points)
        if (rmdir(linkloc) < 0 && errno != ENOENT)
            ereport(ERROR, (errcode_for_file_access(),
                    errmsg("could not remove directory \"%s\": %m", linkloc)));
    }
    else if (S_ISLNK(st.st_mode)) {
        // Symbolic link
        if (unlink(linkloc) < 0 && errno != ENOENT)
            ereport(ERROR, (errcode_for_file_access(),
                    errmsg("could not remove symbolic link \"%s\": %m", linkloc)));
    }
    else {
        // Safety check - refuse to remove unexpected file types
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("\"%s\" is not a directory or symbolic link", linkloc)));
    }
}
```

Key simplifications made:
- Added clear step-by-step process comments
- Explained the platform-specific handling (directories vs symlinks)
- Maintained all error checking and safety measures
- Preserved cross-platform compatibility logic