# adjust_data_dir

## Location
src/bin/pg_ctl/pg_ctl.c: 2113 - 2170

## Overview
A static function in pg_ctl that detects configuration-only directories and resolves the actual PostgreSQL data directory path by querying the PostgreSQL backend.

## Definition


## Detailed Description
The  function handles the scenario where the user has specified a configuration-only directory (containing postgresql.conf but not the actual database files) instead of the real data directory. This situation commonly occurs in PostgreSQL installations where configuration files are separated from data files for administrative or security reasons.

The function performs the following detection and resolution process:
1. **Early exit conditions**: Returns immediately if no pg_config is set
2. **Configuration file check**: Verifies postgresql.conf exists in the specified directory
3. **Data directory detection**: Checks for PG_VERSION file - if present, it's already a real data directory
4. **Backend query**: If it's a config-only directory, executes a postgres backend process with  to query the actual data directory location
5. **Path resolution**: Updates the global pg_data variable with the resolved path and canonicalizes it

The function uses the postgres backend's ability to read configuration files and return the  setting, ensuring accurate resolution even with complex configuration setups.

## Parameters / Member Variables
This function takes no parameters but operates on several global variables.

## Dependencies
- Functions called/Symbols referenced:
  - snprintf (string formatting)
  - fopen, fclose (file operations)
  - [find_other_exec_or_die](../f/find_other_exec_or_die.md) (executable location)
  - [pg_strdup](../p/pg_strdup.md) (memory allocation and string duplication)
  - [psprintf](../p/psprintf.md) (formatted string allocation)
  - popen, pclose (process execution)
  - fgets (line reading)
  - fflush (output flushing)
  - [write_stderr](../w/write_stderr.md) (error output)
  - pg_strip_crlf (line ending cleanup)
  - [canonicalize_path](../c/canonicalize_path.md) (path normalization)
  - free (memory deallocation)
  - exit (program termination)
  - PG_BACKEND_VERSIONSTR (version constant)

- Global variables accessed:
  - [pg_config](../p/pg_config.md), pg_data (directory paths)
  - exec_path, argv0 (executable information)
  - pgdata_opt, post_opts (command-line options)

- Called from (representative examples):
  - [main](../m/main.md) (during pg_ctl initialization in src/bin/pg_ctl/pg_ctl.c:2424)
  - [main](../m/main.md) (in pg_upgrade for data directory resolution)

## Notes and Other Information
- This function is crucial for supporting PostgreSQL installations with separated configuration and data directories
- The detection logic is conservative: it only treats a directory as config-only if postgresql.conf exists but PG_VERSION does not
- Error handling terminates the program if the backend query fails, as this indicates a serious configuration problem
- The function modifies global state by updating pg_data with the resolved directory path
- Memory management includes proper cleanup of allocated strings and process handles
- The backend query uses the  option which must be the first option (as noted in comments)
- Located in src/bin/pg_ctl/pg_ctl.c:2113-2170