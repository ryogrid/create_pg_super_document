# PQparameterStatus

## Location
[src/interfaces/libpq/fe-connect.c:7124-7138](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L7124-L7138)

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
  - [pgParameterStatus](../p/pgParameterStatus.md) (internal structure type)
  - strcmp() (for parameter name comparison)
- Called from (representative examples):
  - [BaseBackup](../B/BaseBackup.md) (pg_basebackup)
  - [CheckServerVersionForStreaming](../C/CheckServerVersionForStreaming.md) (receivelog)
  - [GetConnection](../G/GetConnection.md) (streamutil)
  - [_check_database_version](../c/_check_database_version.md) (pg_dump)
  - [setup_connection](../s/setup_connection.md) (pg_dump)
  - [is_superuser](../i/is_superuser.md) (pg_dump, psql)
  - [connectDatabase](../c/connectDatabase.md) (pg_dumpall)
  - [printVersion](../p/printVersion.md) (pgbench)
  - [connection_warnings](../c/connection_warnings.md) (psql)
  - [SyncVariables](../S/SyncVariables.md) (psql)
  - [standard_strings](../s/standard_strings.md) (psql)
  - [session_username](../s/session_username.md) (psql)
  - [ecpg_build_params](../e/ecpg_build_params.md) (ECPG)

## Notes and Other Information
- Returns NULL if the connection is invalid, parameter name is NULL, or parameter is not found
- Returns a const char* pointing to the parameter value string stored in the connection structure
- The returned pointer should not be modified and remains valid until the connection is closed or the parameter value changes
- Parameter values are automatically updated by the server via ParameterStatus messages
- Commonly queried parameters include server version, encoding settings, timezone, and authentication information
- Essential for applications that need to adapt behavior based on server configuration
- Used extensively by PostgreSQL client tools to determine server capabilities and settings
- The parameter list is maintained as a linked list for efficient updates when servers send parameter change notifications

## Simplified Source

```c
// Simplified version of PQparameterStatus
const char *
PQparameterStatus(const PGconn *conn, const char *paramName)
{
    // Validate input parameters
    if (!conn || !paramName)
        return NULL;

    // Search through parameter status linked list
    for (const pgParameterStatus *pstatus = conn->pstatus;
         pstatus != NULL;
         pstatus = pstatus->next)
    {
        // Return value if parameter name matches
        if (strcmp(pstatus->name, paramName) == 0)
            return pstatus->value;
    }

    // Parameter not found
    return NULL;
}
```

Key simplifications made:
- Added clear comments explaining each logical step
- Enhanced variable declaration for clarity (const qualifier in loop)
- Grouped related logic with comments
- Maintained original algorithm structure while improving readability
- No actual simplification needed as the original function is already quite clean and minimal