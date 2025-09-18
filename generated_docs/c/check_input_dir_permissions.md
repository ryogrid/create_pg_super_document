# check_input_dir_permissions

## Location
src/bin/pg_combinebackup/pg_combinebackup.c: 671 - 684

## Overview
Sets default permissions for new files and directories based on the permissions of a given input directory to ensure consistent permission schemes in the output.

## Definition
```c
static void check_input_dir_permissions(char *dir)
```

## Detailed Description
The check_input_dir_permissions function examines the file permissions of a specified directory and configures the system to use the same permission scheme for newly created files and directories. This ensures that the output directory structure maintains consistent permissions with the input backup directories.

The function performs a simple but important task:
1. **Permission inspection**: Uses stat() to retrieve the file mode and permissions of the specified directory
2. **Permission configuration**: Calls SetDataDirectoryCreatePerm() to configure the global permission settings for new file creation
3. **Error handling**: Provides clear error messages if the directory cannot be accessed

This function is typically called with the final (most recent) input directory to ensure that the combined backup output maintains the same permission characteristics as the source backup.

## Parameters / Member Variables
- `dir`: Path to the directory whose permissions should be used as the template for new file creation

## Dependencies
- Functions called/Symbols referenced:
  - SetDataDirectoryCreatePerm (configure global file creation permissions)
- Called from (representative examples):
  - [main](../m/main.md) (backup processing initialization)

## Notes and Other Information
- Located in src/bin/pg_combinebackup/pg_combinebackup.c:671-684
- Very concise function with a focused single responsibility
- Critical for maintaining proper file system security and consistency
- Uses standard UNIX stat() system call for permission retrieval
- The intent is to match the permission scheme of the final input directory
- Error handling uses PostgreSQL's standard pg_fatal() for consistency with other error reporting