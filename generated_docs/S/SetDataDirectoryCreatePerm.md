# SetDataDirectoryCreatePerm

## Location
[src/common/file_perm.c:34-65](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/file_perm.c#L34-L65)

## Overview
Sets global permission variables for creating files and directories in PostgreSQL's data directory based on the provided directory mode, enabling group access permissions when appropriate.

## Definition
```c
void SetDataDirectoryCreatePerm(int dataDirMode)
```

## Detailed Description
This function configures PostgreSQL's file and directory creation permissions by examining the provided data directory mode. If the data directory has group read/execute permissions (PG_DIR_MODE_GROUP), the function relaxes the creation modes to allow group access on all newly created files and directories. Otherwise, it uses restrictive owner-only permissions. The function sets three global variables that control file system permissions throughout PostgreSQL operations.

## Parameters / Member Variables
- `dataDirMode`: The file mode/permissions of the data directory, typically obtained from stat() system call

## Dependencies
- Functions called/Symbols referenced:
  - PG_DIR_MODE_GROUP (constant for group directory permissions)
  - PG_FILE_MODE_GROUP (constant for group file permissions) 
  - PG_MODE_MASK_GROUP (umask for group permissions)
  - PG_DIR_MODE_OWNER (constant for owner-only directory permissions)
  - PG_FILE_MODE_OWNER (constant for owner-only file permissions)
  - PG_MODE_MASK_OWNER (umask for owner-only permissions)
- Called from (representative examples):
  - [checkDataDir](../c/checkDataDir.md) (src/backend/utils/init/miscinit.c:420)
  - [main](../m/main.md) (src/bin/initdb/initdb.c:3343)
  - [GetDataDirectoryCreatePerm](../G/GetDataDirectoryCreatePerm.md) (src/common/file_perm.c:80)

## Notes and Other Information
- The function modifies global variables pg_dir_create_mode, pg_file_create_mode, and pg_mode_mask
- This is a security-critical function that determines file access controls for the entire PostgreSQL data directory
- The group permissions feature allows multiple users in the same group to access PostgreSQL data files, useful for certain deployment scenarios
- On Windows platforms, Unix-style permissions may not be fully supported, but the function is still called for consistency