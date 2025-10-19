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
  - [strlcpy](../s/strlcpy.md) (safe string copying)
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

## Simplified Source

```c
void create_script_for_old_cluster_deletion(char **deletion_script_file_name)
{
    FILE *script = NULL;
    int tblnum;
    char old_cluster_pgdata[MAXPGPATH], new_cluster_pgdata[MAXPGPATH];

    // Generate script filename
    *deletion_script_file_name = psprintf("%sdelete_old_cluster.%s", SCRIPT_PREFIX, SCRIPT_EXT);

    // Canonicalize paths for comparison
    strlcpy(old_cluster_pgdata, old_cluster.pgdata, MAXPGPATH);
    canonicalize_path(old_cluster_pgdata);
    strlcpy(new_cluster_pgdata, new_cluster.pgdata, MAXPGPATH);
    canonicalize_path(new_cluster_pgdata);

    // Safety check: new data directory should not be inside old directory
    if (path_is_prefix_of_path(old_cluster_pgdata, new_cluster_pgdata)) {
        pg_log(PG_WARNING, "WARNING: new data directory should not be inside the old data directory");
        unlink(*deletion_script_file_name);
        pg_free(*deletion_script_file_name);
        *deletion_script_file_name = NULL;
        return;
    }

    // Safety check: tablespaces should not be inside old cluster data directory
    for (tblnum = 0; tblnum < os_info.num_old_tablespaces; tblnum++) {
        char old_tablespace_dir[MAXPGPATH];
        strlcpy(old_tablespace_dir, os_info.old_tablespaces[tblnum], MAXPGPATH);
        canonicalize_path(old_tablespace_dir);

        if (path_is_prefix_of_path(old_cluster_pgdata, old_tablespace_dir)) {
            pg_log(PG_WARNING, "WARNING: user-defined tablespace locations should not be inside the data directory");
            unlink(*deletion_script_file_name);
            pg_free(*deletion_script_file_name);
            *deletion_script_file_name = NULL;
            return;
        }
    }

    prep_status("Creating script to delete old cluster");

    // Create and write the deletion script
    script = fopen_priv(*deletion_script_file_name, "w");
    if (!script) {
        pg_fatal("could not open file \"%s\": %m", *deletion_script_file_name);
    }

#ifndef WIN32
    fprintf(script, "#!/bin/sh\n\n");
#endif

    // Delete old cluster's default tablespace
    fprintf(script, RMDIR_CMD " %c%s%c\n", PATH_QUOTE,
            fix_path_separator(old_cluster.pgdata), PATH_QUOTE);

    // Delete old cluster's alternate tablespaces
    for (tblnum = 0; tblnum < os_info.num_old_tablespaces; tblnum++) {
        if (strlen(old_cluster.tablespace_suffix) == 0) {
            // Delete per-database directories
            for (int dbnum = 0; dbnum < old_cluster.dbarr.ndbs; dbnum++) {
                fprintf(script, RMDIR_CMD " %c%s%c%u%c\n", PATH_QUOTE,
                        fix_path_separator(os_info.old_tablespaces[tblnum]),
                        PATH_SEPARATOR, old_cluster.dbarr.dbs[dbnum].db_oid, PATH_QUOTE);
            }
        } else {
            // Delete tablespace directory with suffix
            fprintf(script, RMDIR_CMD " %c%s%s%c\n", PATH_QUOTE,
                    fix_path_separator(os_info.old_tablespaces[tblnum]),
                    fix_path_separator(old_cluster.tablespace_suffix), PATH_QUOTE);
        }
    }

    fclose(script);

#ifndef WIN32
    // Make script executable
    chmod(*deletion_script_file_name, S_IRWXU);
#endif

    check_ok();
}
```