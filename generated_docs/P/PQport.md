# PQport

## Location
[src/interfaces/libpq/fe-connect.c:7072-7089](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L7072-L7089)

## Overview
PQport returns the port number associated with a PostgreSQL database connection as a string, providing access to the network port used for the connection.

## Definition
```c
char *PQport(const PGconn *conn)
```

## Detailed Description
PQport is a libpq client library function that retrieves the port number associated with an established PostgreSQL database connection. The function checks the current active host in the connhost array and returns the port value if it is available and non-empty. The port is returned as a string representation rather than a numeric value, maintaining consistency with other libpq connection parameter accessors. This function supports multi-host connection configurations by accessing the port for the currently active host.

## Parameters / Member Variables
- `conn`: A pointer to the PGconn connection object. If NULL, the function returns NULL safely.

## Dependencies
- Functions called/Symbols referenced:
  - None (simple accessor function with conditional logic)
- Called from (representative examples):
  - [libpqrcv_get_senderinfo](../l/libpqrcv_get_senderinfo.md) (src/backend/replication/libpqwalreceiver/libpqwalreceiver.c:434)
  - [main](../m/main.md) (src/bin/pgbench/pgbench.c:7249)
  - [exec_command_conninfo](../e/exec_command_conninfo.md) (src/bin/psql/command.c:689, 692, 698, 701)
  - [do_connect](../d/do_connect.md) (src/bin/psql/command.c:3802, 3812, 3815, 3821, 3824)
  - [SyncVariables](../S/SyncVariables.md) (src/bin/psql/command.c:4055)

## Notes and Other Information
- Returns a pointer to the port string; the caller should not modify or free this string
- Returns NULL if the connection handle is NULL
- Returns an empty string ("") when no port information is available
- [Port](Port.md) is returned as a string, not as a numeric value
- Supports multi-host connection configurations by checking the current active host (whichhost)
- The returned string is valid for the lifetime of the connection object
- Part of the libpq public API for PostgreSQL client applications
- Commonly used in connection information display and logging

## Simplified Source

```c
char *PQport(const PGconn *conn) {
    // Safety check for null connection
    if (!conn)
        return NULL;

    // Return port from current active host if available
    if (conn->connhost != NULL &&
        conn->connhost[conn->whichhost].port != NULL &&
        conn->connhost[conn->whichhost].port[0] != '\0')
        return conn->connhost[conn->whichhost].port;

    return "";
}
```