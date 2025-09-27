# MakePGDirectory

## Location
[src/backend/storage/file/fd.c:3910-3932](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L3910-L3932)

## Overview
A wrapper function for creating PostgreSQL data subdirectories with consistent permissions according to the configured directory creation mode.

## Definition
```c
int MakePGDirectory(const char *directoryName)
```

## Detailed Description
This function provides a standardized way to create directories within the PostgreSQL data directory structure. It ensures that all directories are created with consistent permissions using the global `pg_dir_create_mode` variable, which tracks the correct permissions for PostgreSQL directories. The function is designed for use in backend operations where directories need to be created dynamically, such as during tablespace creation or temporary file operations.

The function is a thin wrapper around the standard `mkdir()` system call but ensures that PostgreSQL's permission policies are consistently applied. This is critical for maintaining security and operational consistency across the database cluster.

## Parameters / Member Variables
- `directoryName`: The path of the directory to be created

## Dependencies
- Functions called/Symbols referenced:
  - mkdir: Standard system call for directory creation
  - pg_dir_create_mode: Global variable defining the correct directory permissions

- Called from (representative examples):
  - [ValidateXLOGDirectoryStructure](../V/ValidateXLOGDirectoryStructure.md): WAL directory structure validation
  - [CreateDirAndVersionFile](../C/CreateDirAndVersionFile.md): Database directory creation
  - [TablespaceCreateDbspace](../T/TablespaceCreateDbspace.md): Tablespace database subdirectory creation
  - [create_tablespace_directories](../c/create_tablespace_directories.md): General tablespace directory creation
  - [CreateSlotOnDisk](../C/CreateSlotOnDisk.md): Replication slot directory creation
  - [copydir](../c/copydir.md): Directory copying operations
  - [PathNameCreateTemporaryDir](../P/PathNameCreateTemporaryDir.md): Temporary directory creation
  - [OpenTemporaryFileInTablespace](../O/OpenTemporaryFileInTablespace.md): Temporary file infrastructure

## Notes and Other Information
- Returns the result of mkdir() - typically 0 on success, -1 on error
- Uses pg_dir_create_mode for consistent permission setting
- Should be preferred over direct mkdir() calls within PostgreSQL backend
- Critical for maintaining proper directory permissions in data directories
- The umask is also set based on file_perm.c for additional permission control
- Improper permissions could cause backup and other processes to fail
- For non-default permissions, direct mkdir() usage should be carefully considered

## Simplified Source

```c
// Simplified version of MakePGDirectory
int MakePGDirectory(const char *directoryName) {
    // Create directory with PostgreSQL's standard permissions
    // Uses pg_dir_create_mode to ensure consistent directory permissions
    return mkdir(directoryName, pg_dir_create_mode);
}
```

Key simplifications made:
- Preserved the core functionality (single mkdir call with pg_dir_create_mode)
- Removed extensive comments while keeping essential purpose clear
- Function is already minimal - main simplification is in documentation clarity
- Focused on the primary purpose: creating directories with consistent PostgreSQL permissions