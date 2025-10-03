# opendir

## Location
[src/port/dirent.c:33-77](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/dirent.c#L33-L77)

## Overview
PostgreSQL's Windows-specific implementation of the POSIX opendir() function that opens a directory stream for reading directory entries.

## Definition

```c
DIR *
opendir(const char *dirname)
```
## Detailed Description
This function is a Windows compatibility implementation of the POSIX opendir() function, located in src/port/dirent.c. It creates and initializes a DIR structure to represent an opened directory stream that can be used with readdir() and closedir(). The implementation handles Windows-specific directory access using Windows API functions like GetFileAttributes() to validate the directory path.

The function performs several validation steps: first checking if the path exists and is actually a directory, then allocating memory for the DIR structure and preparing it for subsequent directory reading operations. It appends a wildcard pattern ("*") to the directory path to enable Windows FindFirstFile/FindNextFile operations.

## Parameters / Member Variables
- `*dirname`: Path to the directory to be opened for reading
## Dependencies
- Functions called/Symbols referenced:
  - [DIR](../D/DIR.md) (Windows directory structure type)
  - malloc (memory allocation)
  - DT_UNKNOWN (directory entry type constant)
  - [dirent](../d/dirent.md) (directory entry structure)

- Called from (representative examples):
  - [AllocateDir](../A/AllocateDir.md) (src/backend/storage/file/fd.c:2858)
  - [CleanupPriorWALFiles](../C/CleanupPriorWALFiles.md) (src/bin/pg_archivecleanup/pg_archivecleanup.c:98)
  - [scan_directory](../s/scan_directory.md) (src/bin/pg_checksums/pg_checksums.c:308)
  - [process_directory_recursively](../p/process_directory_recursively.md) (src/bin/pg_combinebackup/pg_combinebackup.c:932)
  - [sync_pgdata](../s/sync_pgdata.md) (src/common/file_utils.c:149)
  - [walkdir](../w/walkdir.md) (src/common/file_utils.c:278)
  - [pgfnames](../p/pgfnames.md) (src/common/pgfnames.c:45)

## Notes and Other Information
- This is a Windows-specific implementation that provides POSIX compatibility
- The function allocates memory that must be freed by calling closedir()
- Returns NULL on error with appropriate errno values (ENOENT, ENOTDIR, ENOMEM)
- Automatically appends backslash and wildcard (*) to directory path for Windows file search operations
- Sets handle to INVALID_HANDLE_VALUE initially, which gets used by subsequent readdir() calls
- Part of PostgreSQL's portability layer for Windows systems

## Simplified Source

```c
// Simplified version of opendir
DIR *opendir(const char *dirname) {
    DWORD attr;
    DIR *d;

    // Validate directory exists and is actually a directory
    attr = GetFileAttributes(dirname);
    if (attr == INVALID_FILE_ATTRIBUTES) {
        errno = ENOENT;
        return NULL;
    }
    if ((attr & FILE_ATTRIBUTE_DIRECTORY) != FILE_ATTRIBUTE_DIRECTORY) {
        errno = ENOTDIR;
        return NULL;
    }

    // Allocate and initialize DIR structure
    d = malloc(sizeof(DIR));
    if (!d) {
        errno = ENOMEM;
        return NULL;
    }

    // Prepare directory path with wildcard for Windows file search
    d->dirname = malloc(strlen(dirname) + 4);
    if (!d->dirname) {
        errno = ENOMEM;
        free(d);
        return NULL;
    }

    strcpy(d->dirname, dirname);
    // Ensure path ends with backslash
    if (d->dirname[strlen(d->dirname) - 1] != '/' &&
        d->dirname[strlen(d->dirname) - 1] != '\\')
        strcat(d->dirname, "\\");
    strcat(d->dirname, "*");  // Add wildcard for file search

    // Initialize DIR structure fields
    d->handle = INVALID_HANDLE_VALUE;
    d->ret.d_ino = 0;           // no inodes on Windows
    d->ret.d_reclen = 0;        // not used on Windows
    d->ret.d_type = DT_UNKNOWN;

    return d;
}
```

Key simplifications made:
- Preserved all essential logic and error handling
- Maintained Windows-specific validation and setup
- Kept critical memory allocation patterns
- Preserved directory path preparation logic
- Added comments to clarify the main execution steps