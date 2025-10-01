# copydir

## Location
[src/backend/storage/file/copydir.c:37-116](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/copydir.c#L37-L116)

## Overview
Recursively copies a directory and its contents from one location to another, with optional subdirectory recursion and comprehensive data integrity guarantees through filesystem synchronization.

## Definition

```c
struct dirent *xlde;
```
## Detailed Description
The  function provides a robust directory copying mechanism used primarily for database operations like CREATE DATABASE and database relocation. It performs a two-phase copy operation: first copying all files and directories, then ensuring data integrity through strategic filesystem synchronization.

The function operates in the following phases:
1. **Directory Creation**: Creates the destination directory using 
2. **Content Enumeration**: Iterates through all entries in the source directory
3. **Selective Copying**: For regular files, calls ; for subdirectories, recursively calls  if recursion is enabled
4. **Integrity Assurance**: If fsync is enabled, performs a second pass to fsync all copied files and the destination directory itself

The function implements platform-aware optimizations and handles interrupts gracefully through  calls during the copy process.

## Parameters / Member Variables
- : Source directory path to copy from
- : Destination directory path to copy to  
- : Boolean flag controlling whether subdirectories should be recursively copied

## Dependencies
- Functions called/Symbols referenced:
  -  - Creates the destination directory
  -  /  - Directory handle management
  -  - Directory entry enumeration
  -  - Determines file type (directory vs regular file)
  -  - Copies individual regular files
  -  - Synchronizes files and directories to storage
  -  - Handles query cancellation
- Called from (representative examples):
  -  - Database creation via filesystem copy
  -  - Database relocation operations
  -  - WAL replay for database operations
  -  (recursive calls) - Self-recursion for subdirectory copying

## Notes and Other Information
- The function ensures data durability by fsync'ing both individual files and the destination directory itself, which is critical for crash consistency
- Ignores special directory entries (".", "..") and non-regular files/directories  
- Platform-specific behavior controlled by  setting
- Used extensively in database management operations where atomic directory copying with integrity guarantees is essential
- The two-pass fsync approach (files first, then directory) ensures proper ordering of filesystem operations for crash safety

## Simplified Source

```c
void
copydir(const char *fromdir, const char *todir, bool recurse)
{
    DIR *xldir;
    struct dirent *xlde;
    char fromfile[MAXPGPATH * 2];
    char tofile[MAXPGPATH * 2];

    // Create destination directory
    if (MakePGDirectory(todir) != 0)
        ereport(ERROR, "could not create directory");

    // Copy all files and subdirectories
    xldir = AllocateDir(fromdir);
    while ((xlde = ReadDir(xldir, fromdir)) != NULL) {
        CHECK_FOR_INTERRUPTS();

        if (strcmp(xlde->d_name, ".") == 0 || strcmp(xlde->d_name, "..") == 0)
            continue;

        snprintf(fromfile, sizeof(fromfile), "%s/%s", fromdir, xlde->d_name);
        snprintf(tofile, sizeof(tofile), "%s/%s", todir, xlde->d_name);

        PGFileType xlde_type = get_dirent_type(fromfile, xlde, false, ERROR);

        if (xlde_type == PGFILETYPE_DIR) {
            if (recurse)
                copydir(fromfile, tofile, true);  // Recursive copy
        } else if (xlde_type == PGFILETYPE_REG) {
            copy_file(fromfile, tofile);  // Copy regular file
        }
    }
    FreeDir(xldir);

    // Fsync all copied files for durability (if fsync enabled)
    if (!enableFsync)
        return;

    xldir = AllocateDir(todir);
    while ((xlde = ReadDir(xldir, todir)) != NULL) {
        if (strcmp(xlde->d_name, ".") == 0 || strcmp(xlde->d_name, "..") == 0)
            continue;

        snprintf(tofile, sizeof(tofile), "%s/%s", todir, xlde->d_name);
        if (get_dirent_type(tofile, xlde, false, ERROR) == PGFILETYPE_REG)
            fsync_fname(tofile, false);
    }
    FreeDir(xldir);

    // Fsync the destination directory itself
    fsync_fname(todir, true);
}
```