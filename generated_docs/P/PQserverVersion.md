# PQserverVersion

## Location
[src/interfaces/libpq/fe-connect.c:7149-7158](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L7149-L7158)

## Overview
Returns the version number of the PostgreSQL server that the connection is connected to, encoded as an integer for easy version comparison.

## Definition

```c
int
PQserverVersion(const PGconn *conn)
```
## Detailed Description
This function retrieves the PostgreSQL server version number from an established connection. The version is returned as an integer where major, minor, and patch versions are encoded into a single number. For example, version 13.2.1 would be represented as 130201. This encoding allows for easy numerical comparison of server versions to determine feature availability and compatibility.

The function accesses the sversion field from the connection structure, which is populated during the connection establishment process when the server sends its version information.

## Parameters / Member Variables
- `*conn`: A pointer to the PGconn structure representing the database connection. Must not be NULL for valid results.
## Dependencies
- Functions called/Symbols referenced:
  - CONNECTION_BAD (connection status constant)
- Called from (representative examples):
  - [libpqrcv_server_version](../l/libpqrcv_server_version.md) (in replication walreceiver)
  - [CheckServerVersionForStreaming](../C/CheckServerVersionForStreaming.md) (in pg_basebackup)
  - [_check_database_version](../c/_check_database_version.md) (in pg_dump)
  - [printVersion](../p/printVersion.md) (in pgbench)
  - [SyncVariables](../S/SyncVariables.md) (in psql)

## Notes and Other Information
- Returns 0 for invalid connections (NULL pointer or CONNECTION_BAD status)
- The version number format allows easy comparison: newer versions have higher numbers
- Commonly used by client applications to determine which SQL features and protocol capabilities are available
- Essential for tools like pg_dump, pg_basebackup, and psql to adapt their behavior based on server capabilities
- The server version is established during connection setup and remains constant for the connection lifetime

## Simplified Source

```c
int PQserverVersion(const PGconn *conn) {
    // Safety checks for null connection and bad status
    if (!conn)
        return 0;
    if (conn->status == CONNECTION_BAD)
        return 0;

    // Return the server version number
    return conn->sversion;
}
```