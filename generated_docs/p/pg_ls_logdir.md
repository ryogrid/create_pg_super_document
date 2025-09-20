# pg_ls_logdir

## Location
[src/backend/utils/adt/genfile.c:633-639](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/genfile.c#L633-L639)

## Overview
Returns a list of regular files in the PostgreSQL log directory with detailed information including filename, size, and modification time.

## Definition

```c
Datum
pg_ls_logdir(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL system function that provides SQL access to list files in the PostgreSQL log directory. It is a simple wrapper around the  function that specifically targets the log directory configured by the  server setting.

This function returns detailed information about each regular file found in the log directory, including the filename, file size in bytes, and last modification timestamp. The function automatically filters out directories, special files, and hidden files, focusing only on log files that are typically of interest to database administrators.

## Parameters / Member Variables
- : Standard PostgreSQL function call information structure (no user arguments required)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_ls_dir_files](pg_ls_dir_files.md) (the core implementation that performs the actual directory listing)
  - Log_directory (global variable containing the configured log directory path)
- Called from (representative examples):
  - No direct references found (likely called through SQL function dispatch)

## Notes and Other Information
- This function provides SQL access to the PostgreSQL log directory without requiring filesystem-level access
- Uses the server's configured Log_directory setting, which is typically set via postgresql.conf
- Returns a 3-column result set: filename (text), size (bigint), modification time (timestamptz)
- The missing_ok parameter is hardcoded to false, meaning the function will error if the log directory doesn't exist
- Useful for database administrators to monitor log file sizes and ages from within SQL
- Only shows regular files, filtering out directories and special files automatically