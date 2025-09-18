# PQconnectStart

## Location
src/interfaces/libpq/fe-connect.c: 872 - 917

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
  - pqConnectOptions2
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