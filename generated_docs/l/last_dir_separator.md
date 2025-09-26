# last_dir_separator

## Location
[src/port/path.c:144-162](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/path.c#L144-L162)

## Overview
A utility function that locates the last directory separator in a pathname, commonly used to separate directory path from filename components.

## Definition
```c
char *last_dir_separator(const char *filename)
```

## Detailed Description
The last_dir_separator function searches for the last occurrence of a directory separator character in a given filename or path. This function is particularly useful for path manipulation operations where you need to:

1. **Extract the filename**: The portion after the last separator is typically the filename
2. **Extract the directory path**: The portion before the last separator is the directory path
3. **Platform-aware parsing**: Uses skip_drive() to handle Windows drive prefixes correctly before searching

The function iterates through the entire path (after skipping any drive prefix) and keeps track of the last directory separator encountered. It uses the IS_DIR_SEP macro to detect both forward slashes (/) and backslashes (\\) as appropriate for the platform.

This is one of the most commonly used path manipulation functions in PostgreSQL, essential for file and directory operations throughout the codebase.

## Parameters / Member Variables
- `filename`: Input filename or path string to search for the last directory separator

## Dependencies
- Functions called/Symbols referenced:
  - [skip_drive](../s/skip_drive.md) (to bypass drive prefixes)
  - IS_DIR_SEP (macro for checking directory separators)
  - unconstify (macro for casting away const qualifier)
- Called from (representative examples):
  - [sendDir](../s/sendDir.md) (in src/backend/backup/basebackup.c)
  - [setup_bin_paths](../s/setup_bin_paths.md) (in src/bin/initdb/initdb.c)
  - [should_allow_existing_directory](../s/should_allow_existing_directory.md) (in src/bin/pg_basebackup/bbstreamer_file.c)
  - [check_file_excluded](../c/check_file_excluded.md) (in src/bin/pg_rewind/filemap.c)
  - [setup](../s/setup.md) (in src/bin/pg_upgrade/pg_upgrade.c)
  - [find_other_exec](../f/find_other_exec.md) (in src/common/exec.c)
  - [ECPGconnect](../E/ECPGconnect.md) (in src/interfaces/ecpg/ecpglib/connect.c)
  - [main](../m/main.md) (in src/interfaces/ecpg/preproc/ecpg.c)
  - [get_progname](../g/get_progname.md) (in src/port/path.c)

## Notes and Other Information
- Returns NULL if no directory separator is found in the path
- This is a public function available throughout the PostgreSQL codebase
- Commonly used to extract the basename (filename) from a full path by advancing past the returned separator
- Essential for various PostgreSQL utilities including initdb, pg_basebackup, pg_rewind, and pg_upgrade
- The function properly handles both absolute and relative paths by skipping drive prefixes first
- Used in program name extraction, directory validation, and file path manipulation across many PostgreSQL components