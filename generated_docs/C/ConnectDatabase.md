# ConnectDatabase

## Location
src/bin/pg_dump/pg_backup_db.c: 110 - 224

## Overview
Establishes or re-establishes a PostgreSQL database connection for pg_dump/pg_restore operations with comprehensive authentication and security handling.

## Definition
```c
void ConnectDatabase(Archive *AHX, const ConnParams *cparams, bool isReconnect)
```

## Detailed Description
This function is the primary connection establishment routine for PostgreSQL dump and restore utilities. It handles the complete connection lifecycle including password prompting, authentication retry loops, security path configuration, version checking, and signal handling setup. The function supports both initial connections and reconnections, with different behaviors for password prompting in each case. It implements secure practices by setting up search paths and establishing proper signal handlers for graceful cancellation.

## Parameters / Member Variables
- `AHX`: Archive handle that will store the resulting connection and associated metadata
- `cparams`: Connection parameters structure containing host, port, username, database name, and authentication settings
- `isReconnect`: Boolean flag indicating whether this is a reconnection (affects password prompting behavior)

## Dependencies
- Functions called/Symbols referenced:
  - simple_prompt (interactive password prompting)
  - PQconnectdbParams (PostgreSQL connection establishment)
  - PQstatus (connection status checking)
  - PQconnectionNeedsPassword (authentication requirement checking)
  - PQfinish (connection cleanup)
  - ExecuteSqlQueryForSingleRow (SQL execution for security setup)
  - PQconnectionUsedPassword (password usage detection)
  - PQpass (password retrieval)
  - _check_database_version (version compatibility validation)
  - PQsetNoticeProcessor (notice handling setup)
  - set_archive_cancel_info (signal handler configuration)
- Called from (representative examples):
  - RestoreArchive
  - ReconnectToServer
  - CloneArchive
  - main (pg_dump.c)
  - restore_toc_entries_postfork

## Notes and Other Information
- Implements password caching in AH->savedPassword for subsequent connections
- Never prompts for password during reconnections to avoid interactive interruption
- Sets up secure search path using ALWAYS_SECURE_SEARCH_PATH_SQL to prevent SQL injection
- Handles connection string parsing where dbname can contain full connection parameters
- Supports override_dbname to dynamically change target database
- Establishes SIGINT handling for graceful query cancellation
- Validates server version compatibility after successful connection
- Uses libpq's parameter-based connection interface for secure parameter passing