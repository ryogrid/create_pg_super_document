# pg_ls_waldir

## Location
src/backend/utils/adt/genfile.c: 640 - 648

## Overview
Returns a list of regular files in the PostgreSQL WAL (Write-Ahead Log) directory with detailed information including filename, size, and modification time.

## Definition


## Detailed Description
The  function is a PostgreSQL system function that provides SQL access to list files in the PostgreSQL WAL directory. It is a simple wrapper around the  function that specifically targets the WAL directory defined by the  constant.

This function returns detailed information about each regular file found in the WAL directory, including the filename, file size in bytes, and last modification timestamp. The function is particularly useful for monitoring WAL file accumulation, sizes, and ages, which is crucial for database administration tasks like backup management and replication monitoring.

## Parameters / Member Variables
- : Standard PostgreSQL function call information structure (no user arguments required)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_ls_dir_files](pg_ls_dir_files.md) (the core implementation that performs the actual directory listing)
  - XLOGDIR (constant defining the WAL directory path, typically 'pg_wal')
- Called from (representative examples):
  - No direct references found (likely called through SQL function dispatch)

## Notes and Other Information
- This function provides SQL access to the PostgreSQL WAL directory without requiring filesystem-level access
- Uses the XLOGDIR constant which typically resolves to the 'pg_wal' subdirectory of the data directory
- Returns a 3-column result set: filename (text), size (bigint), modification time (timestamptz)
- The missing_ok parameter is hardcoded to false, meaning the function will error if the WAL directory doesn't exist
- Essential for database administrators monitoring WAL file growth and retention
- Only shows regular files, automatically filtering out directories and special files
- Particularly useful for monitoring replication lag and backup requirements based on WAL file accumulation