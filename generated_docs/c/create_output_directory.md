# create_output_directory

## Location
[src/bin/pg_combinebackup/pg_combinebackup.c:718-756](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/pg_combinebackup.c#L718-L756)

## Overview
Creates an output directory for pg_combinebackup operations, ensuring it exists and is empty, while registering it for cleanup on process exit.

## Definition

```c
structs full backups from incrementals.\n\n"), progname);
```
## Detailed Description
This function is responsible for creating and validating output directories used by the pg_combinebackup utility. It performs several key operations:

1. Checks the current state of the target directory using pg_check_dir()
2. Creates the directory if it doesn't exist (unless in dry-run mode)
3. Validates that existing directories are empty to prevent data corruption
4. Registers the directory with the cleanup system for proper cleanup on process exit

The function handles different directory states gracefully, providing appropriate logging and error handling for each scenario. In dry-run mode, it only logs what would be done without making actual changes.

## Parameters / Member Variables
- : String containing the path of the directory to create
- : Pointer to cb_options structure containing operation options including dry_run flag

## Dependencies
- Functions called/Symbols referenced:
  - : Checks directory existence and state
  - : Logs debug messages
  - : Creates directory recursively with proper permissions
  - : Logs fatal error and exits
  - : Registers directory for cleanup
  - : Global variable for directory creation permissions
- Called from (representative examples):
  -  (in src/bin/pg_combinebackup/pg_combinebackup.c:325)
  -  (in src/bin/pg_combinebackup/pg_combinebackup.c:328)

## Notes and Other Information
- The function uses pg_check_dir() return values to determine directory state:
  - 0: Directory doesn't exist
  - 1: Directory exists and is empty
  - 2-4: Directory exists but contains files (treated as error)
  - -1: Directory access error
- In dry-run mode, the function only logs what would be done without creating directories
- The function automatically registers created directories for cleanup, ensuring proper resource management
- Fatal errors are used for invalid directory states to prevent data corruption
- The function is specific to pg_combinebackup utility and handles output directory management for backup combination operations