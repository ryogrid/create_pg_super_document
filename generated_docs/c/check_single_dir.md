# check_single_dir

## Location
[src/bin/pg_upgrade/exec.c:312-340](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/exec.c#L312-L340)

## Overview
This function validates the presence and accessibility of a specific subdirectory within a PostgreSQL data directory, failing with a fatal error if the directory is missing or inaccessible.

## Definition

```c
struct stat statBuf;
```
## Detailed Description
The  function performs essential validation of PostgreSQL data directory structure by checking for the existence and accessibility of specific subdirectories. It constructs the full path by concatenating the base data directory path with the subdirectory name, carefully handling path separators (including Windows-specific considerations where trailing slashes can cause stat() to fail). The function uses stat() to verify both the existence and the directory nature of the path. If the path doesn't exist, isn't accessible, or isn't actually a directory, the function reports a fatal error using report_status(PG_FATAL), which will terminate the pg_upgrade process.

## Parameters / Member Variables
- : Base path to the PostgreSQL data directory
- : Name of the subdirectory to check within the data directory (can be empty string for checking the base directory itself)

## Dependencies
- Functions called/Symbols referenced:
  - snprintf (for path construction)
  - [stat](../s/stat.md) (for file system information)
  - report_status (with PG_FATAL for error reporting)
  - S_ISDIR (macro for checking if the path is a directory)
- Called from (representative examples):
  - [check_data_dir](check_data_dir.md) (called multiple times to validate various PostgreSQL subdirectories)

## Notes and Other Information
- This is a static function, only accessible within the exec.c file
- Handles Windows-specific path issues by avoiding trailing slashes in stat() calls
- The function is used extensively by check_data_dir to validate multiple essential PostgreSQL directories
- [Path](../P/Path.md) construction logic handles both empty subdir (for base directory checking) and non-empty subdir cases
- Fatal errors from this function will terminate the entire pg_upgrade process
- Part of the comprehensive directory validation system in PostgreSQL's pg_upgrade utility
- Essential for ensuring data directory integrity before attempting major version upgrades

## Simplified Source

```c
static void check_single_dir(const char *pg_data, const char *subdir) {
    struct stat statBuf;
    char subDirName[MAXPGPATH];

    // Construct full path, handling empty subdir and Windows path issues
    snprintf(subDirName, sizeof(subDirName), "%s%s%s", pg_data,
             *subdir ? "/" : "",  // Add separator only if subdir is not empty
             subdir);

    // Check if path exists and is accessible
    if (stat(subDirName, &statBuf) != 0) {
        report_status(PG_FATAL, "check for \"%s\" failed: %m", subDirName);
    }
    // Verify it's actually a directory
    else if (!S_ISDIR(statBuf.st_mode)) {
        report_status(PG_FATAL, "\"%s\" is not a directory", subDirName);
    }
}
```