# PQprepare

## Location
src/interfaces/libpq/fe-exec.c: 2306 - 2322

## Overview
Creates a prepared statement by sending a Parse message to the PostgreSQL server, allowing for efficient execution of the same query with different parameters.

## Definition
```c
PGresult *PQprepare(PGconn *conn,
                    const char *stmtName, 
                    const char *query,
                    int nParams, 
                    const Oid *paramTypes)
```

## Detailed Description
PQprepare creates a prepared statement on the PostgreSQL server by issuing a Parse message. This allows the server to parse and plan the query once, which can then be executed multiple times with different parameter values using PQexecPrepared. The prepared statement is stored on the server and associated with the given statement name.

The function follows the same synchronous execution pattern as other libpq exec functions, using PQexecStart for connection preparation, PQsendPrepare to send the Parse message, and PQexecFinish to wait for the server response.

## Parameters / Member Variables
- `conn`: PostgreSQL connection object that must be in a valid state
- `stmtName`: Name to assign to the prepared statement (empty string for unnamed statement)
- `query`: SQL query string with parameter placeholders ($1, $2, etc.)
- `nParams`: Number of parameters the prepared statement will accept
- `paramTypes`: Array of parameter type OIDs, or NULL to let server infer types

## Dependencies
- Functions called/Symbols referenced:
  - PQexecStart
  - PQsendPrepare
  - PQexecFinish
- Called from (representative examples):
  - init_libpq_conn
  - prepareCommand
  - DescribeQuery
  - prepare_common

## Notes and Other Information
- Returns NULL if the preparation request cannot be sent or connection setup fails
- The returned PGresult indicates success or failure of the preparation operation
- Prepared statements persist for the duration of the database session
- Empty stmtName creates an unnamed prepared statement that replaces any previous unnamed statement
- Parameter type inference (paramTypes = NULL) is often sufficient for most use cases
- Prepared statements improve performance for repeatedly executed queries
- The statement name must be unique within the session scope