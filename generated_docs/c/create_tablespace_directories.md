# create_tablespace_directories

## Location
src/backend/commands/tablespace.c: 572 - 685

## Overview
Creates the filesystem infrastructure for a tablespace by establishing directory structures and symlinks between $PGDATA/pg_tblspc/ and the specified location.

## Definition


## Detailed Description
create_tablespace_directories establishes the physical filesystem infrastructure required for a tablespace to function. The function creates a versioned directory structure at the target location and establishes a symbolic link from the PostgreSQL data directory to enable tablespace access.

The function handles two operational modes: normal tablespaces with symbolic links and 'in-place' tablespaces (developer feature) that create directories directly in the data directory. It validates target directory permissions, creates the required version directory to prevent location conflicts, and manages symlink creation with special handling for WAL recovery scenarios.

Permission validation ensures the target directory is accessible and properly secured. The version directory serves as a unique marker preventing multiple tablespaces from using the same location. During recovery, the function removes stale symlinks before creating new ones.

## Parameters / Member Variables
- : Filesystem path where the tablespace should be created
- : OID of the tablespace for generating the symlink name

## Dependencies
- Functions called/Symbols referenced:
  - MakePGDirectory: Creates directories with proper PostgreSQL permissions
  - TABLESPACE_VERSION_DIRECTORY: Constant defining the version subdirectory name
  - S_ISDIR: System macro to verify directory status
  - remove_tablespace_symlink: Removes existing symlinks during recovery
  - symlink: System call to create symbolic links
- Called from (representative examples):
  - CreateTableSpace: During tablespace creation
  - tblspc_redo: During WAL replay for tablespace creation

## Notes and Other Information
- Supports both normal and 'in-place' tablespace creation modes
- Validates and sets appropriate permissions on target directories
- Creates version directory to prevent location conflicts between tablespaces
- Handles special cases during WAL recovery including stale symlink removal
- Uses pg_dir_create_mode for consistent directory permissions
- Prevents multiple tablespaces from sharing the same physical location
- Provides different error messages and hints depending on recovery context