# PQconnectStartParams

## Location
[src/interfaces/libpq/fe-connect.c:791-871](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L791-L871)

## Overview
Begins the asynchronous establishment of a connection to a PostgreSQL backend using connection parameters provided as arrays of keywords and values.

## Definition
```c
PGconn *PQconnectStartParams(const char *const *keywords,
                            const char *const *values,
                            int expand_dbname)
```

## Detailed Description
PQconnectStartParams is the foundational function for creating asynchronous PostgreSQL connections using structured parameter arrays. It performs the initial setup phase including memory allocation, parameter parsing, and connection initialization, but returns immediately without waiting for the connection to complete. The function creates an empty PGconn structure, parses the provided parameter arrays into connection options, validates and stores these options in the connection structure, computes derived connection parameters, and initiates the actual database connection process. Unlike synchronous connection functions, this allows the calling application to perform other work while the connection establishes in the background.

## Parameters / Member Variables
- `keywords`: Array of connection parameter names (null-terminated)
- `values`: Array of corresponding parameter values (null-terminated)
- `expand_dbname`: Flag indicating whether to expand the database name parameter for additional connection options

## Dependencies
- Functions called/Symbols referenced:
  - [pqMakeEmptyPGconn](../p/pqMakeEmptyPGconn.md)
  - [conninfo_array_parse](../c/conninfo_array_parse.md)
  - [fillPGconn](../f/fillPGconn.md)
  - [PQconninfoFree](PQconninfoFree.md)
  - pqConnectOptions2
  - [pqConnectDBStart](../p/pqConnectDBStart.md)
  - CONNECTION_BAD (status constant)
  - [PQconninfoOption](PQconninfoOption.md) (structure type)
- Called from (representative examples):
  - [libpqrcv_connect](../l/libpqrcv_connect.md) (libpqwalreceiver.c)
  - [do_connect](../d/do_connect.md) (psql command.c)
  - [PQconnectdbParams](PQconnectdbParams.md) (fe-connect.c)
  - [PQpingParams](PQpingParams.md) (fe-connect.c)

## Notes and Other Information
- Returns immediately - use PQconnectPoll with select() to monitor connection progress
- Always returns a valid PGconn pointer unless memory allocation fails (returns NULL)
- Check the status field: CONNECTION_BAD indicates an error occurred during setup
- The expand_dbname parameter allows automatic expansion of database connection strings
- All connection parameters are validated and processed before attempting the actual connection
- Error messages are accumulated in the connection structure throughout the process
- Callers must use PQfinish to clean up the connection structure regardless of success/failure
- This is the underlying implementation used by higher-level connection functions