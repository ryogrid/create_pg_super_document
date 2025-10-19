# PQclientEncoding

## Location
[src/interfaces/libpq/fe-connect.c:7247-7254](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L7247-L7254)

## Overview
Returns the client-side character encoding identifier for a PostgreSQL connection.

## Definition

```c
int
PQclientEncoding(const PGconn *conn)
```
## Detailed Description
The PQclientEncoding function retrieves the current client-side character encoding setting for a PostgreSQL connection. Character encoding determines how text data is represented and interpreted between the client application and the PostgreSQL server. This function returns an integer identifier that corresponds to a specific character encoding (such as UTF-8, Latin-1, etc.).

The function first validates that the connection is not null and that the connection status is CONNECTION_OK, ensuring that encoding information is only retrieved from properly established connections. If these conditions are not met, the function returns -1 to indicate an error or unavailable encoding information.

## Parameters / Member Variables
- `*conn`: Pointer to the PGconn connection object from which to retrieve the client encoding. Must be a valid, established connection.
## Dependencies
- Functions called/Symbols referenced:
  - CONNECTION_OK (connection status constant)
- Called from (representative examples):
  - [setup_connection](../s/setup_connection.md) (pg_dump.c)
  - [exec_command_encoding](../e/exec_command_encoding.md) (psql command.c)
  - [SyncVariables](../S/SyncVariables.md) (psql command.c)
  - [SendQuery](../S/SendQuery.md) (psql common.c)
  - [appendStringLiteralConn](../a/appendStringLiteralConn.md) (string_utils.c)
  - Various database utility commands (createdb, dropdb, vacuumdb, etc.)

## Notes and Other Information
- Returns int encoding identifier, or -1 if connection is invalid or not established
- This function is extensively used throughout PostgreSQL client tools for proper text handling and display
- The encoding affects how string literals, identifiers, and query results are processed and displayed
- Used by psql for proper character display and by dump utilities for correct data export
- Part of the core libpq interface for character encoding management
- The actual encoding names can be retrieved using pg_encoding_to_char() with the returned identifier

## Simplified Source

```c
int PQclientEncoding(const PGconn *conn) {
    // Return -1 if no connection or connection not properly established
    if (!conn || conn->status != CONNECTION_OK)
        return -1;

    // Return the client encoding identifier
    return conn->client_encoding;
}
```