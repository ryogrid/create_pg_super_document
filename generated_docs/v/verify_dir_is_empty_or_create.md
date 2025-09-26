# verify_dir_is_empty_or_create

## Location
[src/bin/pg_basebackup/pg_basebackup.c:747-791](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_basebackup.c#L747-L791)

## Overview
Verifies that a specified directory exists and is empty, creating it if it does not exist, or terminating with an error if it exists but contains files.

## Definition
```c
static void verify_dir_is_empty_or_create(char *dirname, bool *created, bool *found)
```

## Detailed Description
This function implements directory validation logic crucial for pg_basebackup operations. It uses `pg_check_dir` to examine the target directory and takes appropriate action based on the directory's state. If the directory does not exist, it creates the directory hierarchy using `pg_mkdir_p`. If the directory exists and is empty (or contains only dot files or mount points), the operation succeeds. However, if the directory exists and contains regular files, the function terminates the program with a fatal error to prevent data corruption or mixing of backup files with existing content. Optional output parameters allow callers to determine whether the directory was created or already existed.

## Parameters / Member Variables
- `dirname`: Path to the directory to verify or create
- `created`: Optional output parameter (can be NULL) - set to true if the directory was created by this function
- `found`: Optional output parameter (can be NULL) - set to true if the directory already existed and was empty

## Dependencies
- Functions called/Symbols referenced:
  - [pg_check_dir](../p/pg_check_dir.md) (examines directory state and contents)
  - [pg_mkdir_p](../p/pg_mkdir_p.md) (creates directory hierarchy with proper permissions)
  - [pg_fatal](../p/pg_fatal.md) (terminates program with error message)
- Global variables accessed:
  - pg_dir_create_mode (directory creation permissions)
- Called from (representative examples):
  - [BaseBackup](../B/BaseBackup.md) function in pg_basebackup.c at line 2083
  - [main](../m/main.md) function in pg_basebackup.c at lines 2849 and 2860

## Notes and Other Information
- The function handles all possible return values from pg_check_dir:
  - 0: Directory does not exist → create it
  - 1: Directory exists and is empty → success
  - 2: Directory contains only dot files → success  
  - 3: Directory contains lost+found (mount point) → error
  - 4: Directory contains regular files → error
  - -1: Access error → error
- Critical safety mechanism preventing accidental overwriting of existing data during backup operations
- Uses pg_fatal for error termination, which provides consistent error formatting and cleanup
- The created and found parameters are optional and can be NULL if the caller doesn't need this information
- Essential for ensuring backup integrity by guaranteeing clean target directories