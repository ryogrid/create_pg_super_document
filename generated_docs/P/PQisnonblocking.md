# PQisnonblocking

## Location
src/interfaces/libpq/fe-exec.c: 3983 - 3991

## Overview
Returns the current blocking status of a PostgreSQL database connection, indicating whether the connection is in non-blocking mode or blocking mode.

## Definition
```c
int PQisnonblocking(const PGconn *conn)
```

## Detailed Description
PQisnonblocking is a query function that returns the current blocking mode status of a PostgreSQL connection. It provides a simple way for applications to determine whether a connection is configured for non-blocking or blocking operations. The function acts as a wrapper around the internal pqIsnonblocking function, adding safety checks for invalid connections.

The function returns a boolean-style integer where:
- Non-zero (true) indicates the connection is in non-blocking mode
- Zero (false) indicates the connection is in blocking mode or is invalid

## Parameters / Member Variables
- `conn`: Constant PostgreSQL connection object (const PGconn pointer) - the connection to query

## Dependencies
- Functions called/Symbols referenced:
  - CONNECTION_BAD (connection status constant)  
  - pqIsnonblocking (internal function that returns the actual blocking status)
- Called from (representative examples):
  - test_disallowed_in_pipeline (in libpq_pipeline test module)
  - test_simple_pipeline (in libpq_pipeline test module)

## Notes and Other Information
- Returns false (0) if the connection is NULL or in CONNECTION_BAD state
- This is a read-only query function that does not modify connection state
- Uses const qualifier to indicate it does not modify the connection object
- Commonly used in pipeline and asynchronous processing scenarios to verify connection mode
- Safe to call at any time during connection lifetime
- The function serves as the public API counterpart to PQsetnonblocking for querying blocking status