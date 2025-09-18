# PQsetnonblocking

## Location
[src/interfaces/libpq/fe-exec.c:3944-3982](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L3944-L3982)

## Overview
Sets the PostgreSQL connection to non-blocking mode if the argument is true, or to blocking mode if the argument is false. This function provides control over the blocking behavior of database operations.

## Definition
```c
int PQsetnonblocking(PGconn *conn, int arg)
```

## Detailed Description
PQsetnonblocking controls whether database operations on the connection will block or return immediately. When set to non-blocking mode (arg = true), operations will return immediately even if they cannot complete, allowing the application to handle other tasks while waiting for database operations to finish. When set to blocking mode (arg = false), operations will wait until they complete before returning.

The function includes important safeguards:
- It flushes the send queue before changing modes to guarantee proper behavior consistency
- It clears error state unless actively pipelining to ensure clean state transitions
- It performs early exit optimization if the connection is already in the requested state

Note that this function only affects non-blocking API operations; it does not protect against blocking behavior when using PQexec().

## Parameters / Member Variables
- `conn`: PostgreSQL connection object (PGconn pointer)
- `arg`: Integer flag where non-zero sets non-blocking mode, zero sets blocking mode

## Dependencies
- Functions called/Symbols referenced:
  - CONNECTION_BAD (connection status constant)
  - pqClearConnErrorState (clears connection error state)
  - [pqFlush](../p/pqFlush.md) (flushes pending data)
- Called from (representative examples):
  - [test_cancel](../t/test_cancel.md) (in libpq_pipeline test module)
  - [test_pipelined_insert](../t/test_pipelined_insert.md) (in libpq_pipeline test module)
  - [test_uniqviol](../t/test_uniqviol.md) (in libpq_pipeline test module)

## Notes and Other Information
- Returns 0 on success, -1 on failure
- Should only be called on an active database connection
- Returns -1 immediately if connection is NULL or in CONNECTION_BAD state
- The function performs a flush operation which may block regardless of the target mode
- Primarily used with pipelining and asynchronous query processing
- Does not affect PQexec() behavior - use non-blocking API functions for true non-blocking operation