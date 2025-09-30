# directory_is_empty

## Location
[src/backend/commands/tablespace.c:853-882](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablespace.c#L853-L882)

## Overview
Checks if a directory is empty by iterating through its contents and returning true if no files or subdirectories (other than "." and "..") are found.

## Definition

```c
struct dirent *de;
```
## Detailed Description
This utility function verifies whether a specified directory contains any files or subdirectories. It opens the directory using PostgreSQL's directory handling functions and reads through all entries, skipping the standard "." (current directory) and ".." (parent directory) entries. The function returns true if the directory contains no other entries, and false if any files or subdirectories are found. The function properly handles resource cleanup by ensuring the directory handle is freed in all code paths.

## Parameters / Member Variables
- : The filesystem path to the directory to be checked for emptiness

## Dependencies
- Functions called/Symbols referenced:
  - [AllocateDir](../A/AllocateDir.md): Opens a directory for reading
  - [ReadDir](../R/ReadDir.md): Reads the next directory entry
  - [FreeDir](../F/FreeDir.md): Closes and frees directory resources
  - [DIR](../D/DIR.md): Directory handle structure
  - [dirent](dirent.md): Directory entry structure

- Called from (representative examples):
  - [CreateDatabaseUsingFileCopy](../C/CreateDatabaseUsingFileCopy.md): Used during database creation operations
  - [createdb](../c/createdb.md): Database creation command implementation
  - [destroy_tablespace_directories](destroy_tablespace_directories.md): Tablespace cleanup operations
  - [pg_tablespace_databases](../p/pg_tablespace_databases.md): System function for tablespace database listing

## Notes and Other Information
- The function comment indicates uncertainty about the most appropriate location for this utility function within the codebase
- Proper resource management is implemented with FreeDir() called in both success and failure paths
- This function is primarily used in database and tablespace management operations where directory state verification is required
- The function handles standard Unix directory entries ("." and "..") by explicitly ignoring them

## Simplified Source

```c
bool directory_is_empty(const char *path) {
    DIR *dirdesc;
    struct dirent *de;

    // Open directory for reading
    dirdesc = AllocateDir(path);

    // Check each directory entry
    while ((de = ReadDir(dirdesc, path)) != NULL) {
        // Skip standard directory entries
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
            continue;

        // Found a real file or subdirectory - directory is not empty
        FreeDir(dirdesc);
        return false;
    }

    // No entries found (except . and ..) - directory is empty
    FreeDir(dirdesc);
    return true;
}
```