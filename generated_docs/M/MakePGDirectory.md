# MakePGDirectory

## Location
src/backend/storage/file/fd.c: 3910 - 3932

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
  - ValidateXLOGDirectoryStructure: WAL directory structure validation
  - CreateDirAndVersionFile: Database directory creation
  - TablespaceCreateDbspace: Tablespace database subdirectory creation
  - create_tablespace_directories: General tablespace directory creation
  - CreateSlotOnDisk: Replication slot directory creation
  - copydir: Directory copying operations
  - PathNameCreateTemporaryDir: Temporary directory creation
  - OpenTemporaryFileInTablespace: Temporary file infrastructure

## Notes and Other Information
- Returns the result of mkdir() - typically 0 on success, -1 on error
- Uses pg_dir_create_mode for consistent permission setting
- Should be preferred over direct mkdir() calls within PostgreSQL backend
- Critical for maintaining proper directory permissions in data directories
- The umask is also set based on file_perm.c for additional permission control
- Improper permissions could cause backup and other processes to fail
- For non-default permissions, direct mkdir() usage should be carefully considered