# connectFailureMessage

## Location
[src/interfaces/libpq/fe-connect.c:2149-2168](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L2149-L2168)

## Overview
Creates a user-friendly error message for connection failures by combining system error information with helpful diagnostic suggestions based on the connection type.

## Definition
```c
static void connectFailureMessage(PGconn *conn, int errorno)
```

## Detailed Description
This function generates a comprehensive error message when a connection attempt fails due to an errno condition that suggests no server is present at the target location. It combines the system error description with contextual advice:

- For Unix domain socket connections: Suggests checking if the server is running locally and accepting socket connections
- For TCP/IP connections: Suggests checking if the server is running on the target host and accepting TCP/IP connections

The function appends both the error description and diagnostic hint to the connection's error message buffer, providing users with actionable troubleshooting information.

## Parameters / Member Variables
- `conn`: Pointer to the PGconn connection object containing the error message buffer and connection state
- `errorno`: The errno value from the failed connection attempt, used to generate the system error description

## Dependencies
- Functions called/Symbols referenced:
  - PG_STRERROR_R_BUFLEN (buffer size constant for error strings)
  - SOCK_STRERROR (macro for thread-safe error string conversion)
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md) (for appending diagnostic suggestions)
- Called from (representative examples):
  - CONNECTION_FAILED (connection failure handling state)

## Notes and Other Information
- This is a static function internal to fe-connect.c
- Designed specifically for errors that indicate "no server there" rather than authentication or other connection issues
- Provides different diagnostic messages based on connection family (Unix socket vs TCP/IP)
- Uses thread-safe error string conversion via SOCK_STRERROR macro
- Part of libpq's user-friendly error reporting system