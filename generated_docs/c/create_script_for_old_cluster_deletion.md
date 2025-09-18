# create_script_for_old_cluster_deletion

## Location
[src/bin/pg_upgrade/check.c:914-1036](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/check.c#L914-L1036)

## Overview
This function generates a shell script that safely removes old PostgreSQL cluster directories and tablespaces after a successful upgrade, providing users with a convenient way to clean up obsolete data.

## Definition
```c
void create_script_for_old_cluster_deletion(char **deletion_script_file_name)
```

## Detailed Description
The `create_script_for_old_cluster_deletion` function creates a platform-specific shell script (delete_old_cluster.sh on Unix, delete_old_cluster.bat on Windows) that contains the necessary commands to remove the old PostgreSQL cluster's data directory and associated tablespaces. This script is particularly useful for safely cleaning up after a successful pg_upgrade operation.

The function performs several important safety checks:
1. Validates that the new data directory is not inside the old data directory
2. Ensures user-defined tablespaces are not located within the old cluster data directory
3. Handles different tablespace suffix scenarios (version-specific vs. database-specific)

The generated script includes:
- Shebang header for Unix systems
- Remove commands for the old cluster's default tablespace
- Remove commands for all alternate tablespaces
- Proper path quoting and separator handling for cross-platform compatibility

If safety conditions are not met, the function issues warnings and returns without creating a script.

## Parameters / Member Variables
- `deletion_script_file_name`: Double pointer to char that will be set to the generated script filename, or NULL if script creation is skipped

## Dependencies
- Functions called/Symbols referenced:
  - [psprintf](../p/psprintf.md) (string formatting with memory allocation)
  - strlcpy (safe string copying)
  - [canonicalize_path](canonicalize_path.md) (path normalization)
  - [path_is_prefix_of_path](../p/path_is_prefix_of_path.md) (path relationship checking)
  - [pg_log](../p/pg_log.md) (logging with severity levels)
  - unlink (file deletion)
  - [pg_free](../p/pg_free.md) (memory deallocation)
  - [prep_status](../p/prep_status.md) (status reporting)
  - fopen_priv (secure file opening)
  - fclose (file closing)
  - chmod (file permission setting)
  - [check_ok](check_ok.md) (completion status reporting)
  - [fix_path_separator](../f/fix_path_separator.md) (path separator normalization)
- Called from (representative examples):
  - [main](../m/main.md) (from pg_upgrade.c main function)

## Notes and Other Information
- This is a public function accessible from other compilation units
- Creates executable scripts on Unix systems using chmod with S_IRWXU permissions
- Uses platform-specific constants (RMDIR_CMD, PATH_QUOTE, PATH_SEPARATOR, SCRIPT_EXT)
- Handles both version-specific and database-specific tablespace scenarios
- Provides comprehensive safety checks to prevent accidental data deletion
- Script filename follows pattern: delete_old_cluster.{sh|bat}
- Function may return early with NULL script name if safety conditions are not met