# read_pg_version_file

## Location
src/bin/pg_combinebackup/pg_combinebackup.c: 1154 - 1204

## Overview
Reads the PostgreSQL version number from the PG_VERSION file in a specified directory and converts it to the standard server version number format used internally.

## Definition
```c
static int read_pg_version_file(char *directory)
```

## Detailed Description
This function constructs the path to the PG_VERSION file within the given directory, reads its contents, and parses the version number string. The function converts the version string (e.g., "14\n") to PostgreSQL's internal version numbering scheme by multiplying by 10000 (e.g., 140000). It includes error handling for file operations and version parsing, with specific checks to reject very old PostgreSQL versions that used multi-part version numbers (like 9.6 or 8.4) as they are not relevant to incremental backup functionality.

## Parameters / Member Variables
- `directory`: Path to the directory containing the PG_VERSION file to read

## Dependencies
- Functions called/Symbols referenced:
  - open (system call for file opening)
  - [slurp_file](../s/slurp_file.md) (utility function to read file contents into StringInfo)
  - close (system call for file closing)
  - pg_log_debug (logging function for debug output)
- Called from (representative examples):
  - [main](../m/main.md) (in src/bin/pg_combinebackup/pg_combinebackup.c:269)

## Notes and Other Information
- This is a static function used specifically within pg_combinebackup utility
- The function enforces a length limit of 128 bytes when reading the PG_VERSION file
- Returns version number in PostgreSQL's internal format (major version * 10000)
- Includes specific error handling for old PostgreSQL versions with multi-part version numbers
- Uses StringInfo for safe string handling and memory management
- File location: src/bin/pg_combinebackup/pg_combinebackup.c:1154-1204