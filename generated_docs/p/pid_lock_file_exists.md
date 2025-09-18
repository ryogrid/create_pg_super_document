# pid_lock_file_exists

## Location
src/bin/pg_upgrade/exec.c: 233 - 262

## Overview
This function checks whether the postmaster.pid lock file exists in a PostgreSQL data directory to determine if a PostgreSQL server instance is potentially running.

## Definition


## Detailed Description
The  function is used during PostgreSQL upgrade operations to verify whether a PostgreSQL server instance might be running in a given data directory. It attempts to open the  file in read-only mode. If the file can be opened successfully, it indicates that a postmaster process may be running (or was running recently and did not clean up properly). The function handles specific error cases gracefully - if the file doesn't exist (ENOENT) or the directory path is invalid (ENOTDIR), it returns false without throwing an error, as these are expected conditions when no server is running.

## Parameters / Member Variables
- : Path to the PostgreSQL data directory where the postmaster.pid file should be checked

## Dependencies
- Functions called/Symbols referenced:
  - snprintf
  - open
  - close
  - pg_fatal (for unexpected errors)
- Called from (representative examples):
  - setup (in pg_upgrade.c, called twice for old and new clusters)

## Notes and Other Information
- Returns true if the postmaster.pid file exists and can be opened, false otherwise
- Used specifically in pg_upgrade to ensure that PostgreSQL instances are shut down before attempting an upgrade
- The function only checks for file existence, not whether the PID in the file corresponds to a running process
- Handles ENOTDIR and ENOENT errors as normal conditions, but treats other open() failures as fatal errors
- Part of PostgreSQL's pg_upgrade utility for major version upgrades