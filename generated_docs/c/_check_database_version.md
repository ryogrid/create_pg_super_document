# _check_database_version

## Location
src/bin/pg_dump/pg_backup_db.c: 33 - 73

## Overview
Validates PostgreSQL server version compatibility and determines standby status for pg_dump operations.

## Definition


## Detailed Description
This internal function performs critical version compatibility checks when pg_dump connects to a PostgreSQL server. It retrieves the server version information, validates that the server version is within the acceptable range for the current pg_dump version, and determines if the server is in recovery mode (hot standby). If version compatibility fails, the function terminates the program with an error message.

## Parameters / Member Variables
- `AH`: Archive handle containing connection information and version constraints for the dump operation

## Dependencies
- Functions called/Symbols referenced:
  - PQparameterStatus (retrieves server version string)
  - [PQserverVersion](../P/PQserverVersion.md) (retrieves numeric server version)
  - [pg_fatal](../p/pg_fatal.md) (error handling for libpq failures)
  - [pg_strdup](../p/pg_strdup.md) (string duplication)
  - pg_log_error (error logging)
  - pg_log_error_detail (detailed error logging)
  - [ExecuteSqlQueryForSingleRow](../E/ExecuteSqlQueryForSingleRow.md) (executes SQL query for standby check)
  - [PQgetvalue](../P/PQgetvalue.md) (extracts result value)
  - [PQclear](../P/PQclear.md) (cleanup result set)
- Called from (representative examples):
  - [ConnectDatabase](../C/ConnectDatabase.md)

## Notes and Other Information
- Sets AH->public.remoteVersionStr and AH->public.remoteVersion for later use
- Checks if server version is within minRemoteVersion and maxRemoteVersion bounds
- Determines standby status by querying pg_catalog.pg_is_in_recovery()
- Terminates program execution on version mismatch rather than returning error code
- Part of pg_dump's connection establishment and validation process