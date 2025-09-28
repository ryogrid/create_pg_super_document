# PQconnectStart

## Location
[src/interfaces/libpq/fe-connect.c:872-917](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L872-L917)

## Overview
Begins the asynchronous establishment of a connection to a PostgreSQL backend using a connection information string, providing the foundation for non-blocking database connections.

## Definition
```c
PGconn *PQconnectStart(const char *conninfo)
```

## Detailed Description
PQconnectStart is a convenience wrapper around PQconnectStartParams that accepts a traditional connection string instead of parameter arrays. It performs the same asynchronous connection initialization process: allocating a PGconn structure, parsing the connection string into connection options, computing derived connection parameters, and initiating the database connection process. The function returns immediately without waiting for the connection to complete, allowing applications to perform other work while the connection establishes. The connection string format is identical to that used by PQconnectdb, supporting both key=value pairs and URI notation.

## Parameters / Member Variables
- `conninfo`: A connection string containing database connection parameters in either space-separated "key=value" format or PostgreSQL URI format

## Dependencies
- Functions called/Symbols referenced:
  - [pqMakeEmptyPGconn](../p/pqMakeEmptyPGconn.md)
  - [connectOptions1](../c/connectOptions1.md)
  - [pqConnectOptions2](../p/pqConnectOptions2.md)
  - [pqConnectDBStart](../p/pqConnectDBStart.md)
  - CONNECTION_BAD (status constant)
- Called from (representative examples):
  - [PQconnectdb](PQconnectdb.md) (fe-connect.c)
  - [PQping](PQping.md) (fe-connect.c)
  - [libpqsrv_connect](../l/libpqsrv_connect.md) (libpq-be-fe-helpers.h)

## Notes and Other Information
- This is the string-based equivalent of PQconnectStartParams for asynchronous connections
- Returns immediately - use PQconnectPoll with select() to monitor connection progress
- Always returns a valid PGconn pointer unless memory allocation fails (returns NULL)
- Check the status field: CONNECTION_BAD indicates an error occurred during initialization
- Used internally by both PQconnectdb (for synchronous connections) and PQping (for server checks)
- The connection string is parsed by connectOptions1 which handles both traditional and URI formats
- All error messages are accumulated in the PGconn structure for later inspection
- Callers must use PQfinish to clean up the connection regardless of success or failure
- Forms the basis for most PostgreSQL client connection scenarios in libpq

## Simplified Source

```c
// Simplified version of PQconnectStart
PGconn *PQconnectStart(const char *conninfo) {
    // Allocate and initialize empty connection structure
    PGconn *conn = pqMakeEmptyPGconn();
    if (conn == NULL)
        return NULL;

    // Parse connection string into connection options
    if (!connectOptions1(conn, conninfo))
        return conn;  // Error details stored in conn->errorMessage

    // Compute derived connection parameters
    if (!pqConnectOptions2(conn))
        return conn;  // Error details stored in conn->errorMessage

    // Start the actual database connection process
    if (!pqConnectDBStart(conn)) {
        conn->status = CONNECTION_BAD;
    }

    return conn;  // Always returns valid PGconn, check status for success
}
```

Key simplifications made:
- Removed detailed comments and kept essential ones
- Combined variable declaration with assignment
- Simplified error handling logic
- Focused on the main connection initialization flow
- Emphasized that conn is always returned (unless malloc fails)