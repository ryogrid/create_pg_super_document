# check_file_excluded

## Location
[src/bin/pg_rewind/filemap.c:409-472](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/filemap.c#L409-L472)

## Overview
A static function that determines whether a given file path should be excluded from pg_rewind processing based on predefined exclusion rules for temporary files, cache files, and runtime directories.

## Definition
static bool check_file_excluded(const char *path, bool is_source)

## Detailed Description
This function implements pg_rewind's file exclusion logic by checking whether a given file or directory should be skipped during the rewind process. It applies a comprehensive set of exclusion rules designed to avoid processing files that are either temporary, automatically regenerated, or not essential for database consistency.

The function performs three levels of exclusion checking:
1. **Temporary files**: Automatically excludes all temporary files matching PostgreSQL's temporary file patterns (pgsql_tmp prefix and pgsql_tmp directories)
2. **Individual files**: Checks against a predefined list of specific files that should be excluded (like postgresql.auto.conf.tmp, postmaster.pid, etc.)
3. **Directory contents**: Excludes entire directory trees for directories like pg_stat_tmp, pg_replslot, pg_dynshmem, etc.

The exclusion rules are maintained in sync with basebackup.c to ensure consistency across PostgreSQL backup and recovery tools. The function also provides debug logging to track which files are being excluded and from which system (source or target).

## Parameters / Member Variables
- `path`: The file or directory path to check for exclusion (relative to PostgreSQL data directory)
- `is_source`: Boolean flag indicating whether this is a source file (true) or target file (false), used for logging purposes

## Dependencies
- Functions called/Symbols referenced:
  - PG_TEMP_FILE_PREFIX (constant for temporary file prefix)
  - PG_TEMP_FILES_DIR (constant for temporary files directory)
  - excludeFiles (static array of exclude_list_item structures)
  - excludeDirContents (static array of directory names to exclude)
  - [last_dir_separator](../l/last_dir_separator.md) (function to find last directory separator in path)
  - pg_log_debug (logging function)
  - strstr (string search function)
  - strncmp (string comparison function)
  - strlen (string length function)
  - snprintf (formatted string printing function)
- Called from (representative examples):
  - [decide_file_action](../d/decide_file_action.md) (in filemap.c:718)

## Notes and Other Information
- This is a static function, only accessible within the filemap.c file
- The exclusion lists are maintained with "best effort" to stay synchronized with basebackup.c
- Excluded directories include: pg_stat_tmp, pg_replslot, pg_dynshmem, pg_notify, pg_serial, pg_snapshots, pg_subtrans
- Excluded files include: postgresql.auto.conf.tmp, current_logfiles.tmp, pg_internal.init (prefix match), backup_label, tablespace_map, backup_manifest, postmaster.pid, postmaster.opts
- Supports both exact filename matching and prefix matching for files
- Provides different debug log messages for source vs target exclusions
- Critical for avoiding conflicts with files that are automatically managed by PostgreSQL server startup/shutdown processes
- Helps ensure that pg_rewind only processes files that are actually relevant for database state synchronization

## Simplified Source

```c
static bool check_file_excluded(const char *path, bool is_source)
{
    char local_path[MAXPGPATH];
    int exclude_index;
    const char *filename;

    // Skip all temporary files (pgsql_tmp directories and files)
    if (strstr(path, "/" PG_TEMP_FILE_PREFIX) != NULL ||
        strstr(path, "/" PG_TEMP_FILES_DIR "/") != NULL)
    {
        return true;
    }

    // Check against list of individual excluded files
    for (exclude_index = 0; excludeFiles[exclude_index].name != NULL; exclude_index++)
    {
        int compare_length = strlen(excludeFiles[exclude_index].name);

        // Extract filename from full path
        filename = last_dir_separator(path);
        if (filename == NULL)
            filename = path;
        else
            filename++;

        // Adjust comparison length based on prefix matching rules
        if (!excludeFiles[exclude_index].match_prefix)
            compare_length++;

        // Check if filename matches excluded file pattern
        if (strncmp(filename, excludeFiles[exclude_index].name, compare_length) == 0)
        {
            if (is_source)
                pg_log_debug("entry \"%s\" excluded from source file list", path);
            else
                pg_log_debug("entry \"%s\" excluded from target file list", path);
            return true;
        }
    }

    // Check against directories whose contents should be completely excluded
    for (exclude_index = 0; excludeDirContents[exclude_index] != NULL; exclude_index++)
    {
        snprintf(local_path, sizeof(local_path), "%s/",
                excludeDirContents[exclude_index]);

        // Check if path starts with excluded directory
        if (strstr(path, local_path) == path)
        {
            if (is_source)
                pg_log_debug("entry \"%s\" excluded from source file list", path);
            else
                pg_log_debug("entry \"%s\" excluded from target file list", path);
            return true;
        }
    }

    return false;
}
```