# CheckServerVersionForStreaming

## Location
src/bin/pg_basebackup/receivelog.c: 375 - 452

## Overview
Validates that the PostgreSQL server version is compatible with the streaming replication functionality before attempting to receive transaction log streams.

## Definition


## Detailed Description
The  function performs version compatibility checks to ensure that the connected PostgreSQL server supports the streaming replication protocol used by the client tools. It enforces both minimum and maximum version constraints: the server must be at least version 9.3 (where the streaming replication message format was standardized) and cannot be newer than the client version (to avoid protocol incompatibilities). If the version check fails, appropriate error messages are logged and the function returns false to prevent streaming attempts that would fail.

This function is essential for preventing runtime errors and ensuring reliable streaming replication connections across different PostgreSQL versions.

## Parameters / Member Variables
- : PostgreSQL connection handle to check the server version against

## Dependencies
- Functions called/Symbols referenced:
  - PQserverVersion
  - PQparameterStatus
- Called from (representative examples):
  - BaseBackup
  - StreamLog  
  - ReceiveXlogStream

## Notes and Other Information
- Minimum supported server version is 9.3 (version number 903) due to streaming replication message format changes
- Maximum supported server version is the client's PostgreSQL version to ensure protocol compatibility
- Uses PQparameterStatus to get human-readable server version string for error messages
- Critical safety check that prevents incompatible streaming attempts
- Returns boolean value indicating compatibility status for calling functions to handle appropriately