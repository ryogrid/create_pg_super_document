# PQcancelSocket

## Location
[src/interfaces/libpq/fe-cancel.c:295-306](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-cancel.c#L295-L306)

## Overview
Retrieves the socket file descriptor from a cancel connection, allowing direct access to the underlying socket for query cancellation operations.

## Definition

```c
int
PQcancelSocket(const PGcancelConn *cancelConn)
```
## Detailed Description
PQcancelSocket is a utility function that extracts the socket file descriptor from a PostgreSQL cancel connection object. It serves as a wrapper around PQsocket, specifically designed to work with PGcancelConn structures used for query cancellation. This function enables applications to access the underlying socket for advanced socket operations or monitoring during the cancellation process.

## Parameters / Member Variables
- : A pointer to a constant PGcancelConn structure representing the cancel connection from which to extract the socket

## Dependencies
- Functions called/Symbols referenced:
  - [PQsocket](PQsocket.md)
  - PGcancelConn (type)
- Called from (representative examples):
  - [test_cancel](../t/test_cancel.md) (in libpq_pipeline test module)
  - [libpqsrv_cancel](../l/libpqsrv_cancel.md) (libpq backend-frontend helpers)

## Notes and Other Information
- Returns the socket file descriptor as an integer
- The function provides a type-safe way to access the socket from a cancel connection
- Primarily used in testing scenarios and backend-frontend helper functions
- The socket can be used for polling, monitoring connection status, or implementing custom timeout mechanisms during query cancellation