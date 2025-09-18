# RetrieveDataDirCreatePerm

## Location
src/bin/pg_basebackup/streamutil.c: 426 - 479

## Overview
Retrieves the data directory permissions from the PostgreSQL server and configures local file/directory creation permissions accordingly.

## Definition


## Detailed Description
RetrieveDataDirCreatePerm determines the permission mode of the PostgreSQL data directory on the server side and configures the local environment to create files and directories with matching permissions. This function is particularly important for PostgreSQL 11+ which introduced support for optional group read/execute rights on the data directory. For earlier versions, it maintains default group access settings. The function executes "SHOW data_directory_mode" to retrieve the octal permission value and applies it via SetDataDirectoryCreatePerm.

## Parameters / Member Variables
- : PGconn pointer to an active PostgreSQL connection

## Dependencies
- Functions called/Symbols referenced:
  - PQserverVersion
  - PQexec
  - PQresultStatus
  - PQntuples
  - PQnfields
  - PQgetvalue
  - PQclear
  - SetDataDirectoryCreatePerm
  - sscanf
  - pg_log_error
  - MINIMUM_VERSION_FOR_GROUP_ACCESS
  - PGRES_TUPLES_OK
- Called from (representative examples):
  - GetConnection

## Notes and Other Information
- Returns true on success, false on failure
- This is a static function, only accessible within streamutil.c
- For PostgreSQL versions before 11, returns true immediately (uses default permissions)
- Parses the data_directory_mode as an octal value representing Unix file permissions
- Critical for ensuring that backup files and directories are created with appropriate permissions matching the source server
- The retrieved permissions are applied globally via SetDataDirectoryCreatePerm for subsequent file/directory creation operations
- Part of the security model ensuring proper access control for backup operations