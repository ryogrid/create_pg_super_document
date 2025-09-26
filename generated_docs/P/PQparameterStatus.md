# PQparameterStatus

## Location
src/interfaces/libpq/fe-connect.c: 7124 - 7138

## Overview
PQparameterStatus retrieves the current value of a server parameter that was reported by the PostgreSQL server during connection establishment or via parameter change notifications.

## Definition
```c
const char *PQparameterStatus(const PGconn *conn, const char *paramName)
```

## Detailed Description
This function searches through the parameter status list maintained by the connection object to find the current value of a specified server parameter. PostgreSQL servers automatically report certain parameter values during connection establishment and send parameter status updates when these values change during the session.

The function performs a linear search through a linked list of pgParameterStatus structures, comparing parameter names using strcmp() until it finds a match or reaches the end of the list. Common parameters that can be queried include server_version, server_encoding, client_encoding, application_name, is_superuser, session_authorization, DateStyle, IntervalStyle, TimeZone, integer_datetimes, and standard_conforming_strings.

## Parameters / Member Variables
- `conn`: A pointer to a PGconn structure representing the database connection. If NULL, the function returns NULL.
- `paramName`: A null-terminated string specifying the name of the parameter to query. If NULL, the function returns NULL.

## Dependencies
- Functions called/Symbols referenced:
  - pgParameterStatus (internal structure type)
  - strcmp() (for parameter name comparison)
- Called from (representative examples):
  - BaseBackup (pg_basebackup)
  - CheckServerVersionForStreaming (receivelog)
  - GetConnection (streamutil)
  - _check_database_version (pg_dump)
  - setup_connection (pg_dump)
  - is_superuser (pg_dump, psql)
  - connectDatabase (pg_dumpall)
  - printVersion (pgbench)
  - connection_warnings (psql)
  - SyncVariables (psql)
  - standard_strings (psql)
  - session_username (psql)
  - ecpg_build_params (ECPG)

## Notes and Other Information
- Returns NULL if the connection is invalid, parameter name is NULL, or parameter is not found
- Returns a const char* pointing to the parameter value string stored in the connection structure
- The returned pointer should not be modified and remains valid until the connection is closed or the parameter value changes
- Parameter values are automatically updated by the server via ParameterStatus messages
- Commonly queried parameters include server version, encoding settings, timezone, and authentication information
- Essential for applications that need to adapt behavior based on server configuration
- Used extensively by PostgreSQL client tools to determine server capabilities and settings
- The parameter list is maintained as a linked list for efficient updates when servers send parameter change notifications