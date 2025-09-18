# PQsendQueryContinue

## Location
src/interfaces/libpq/fe-exec.c: 1422 - 1427

## Overview
Submits a SQL query to the PostgreSQL server asynchronously without resetting the connection's error message, preserving any existing error context for continued error handling.

## Definition
```c
int PQsendQueryContinue(PGconn *conn, const char *query)
```

## Detailed Description
PQsendQueryContinue is a specialized variant of PQsendQuery that behaves identically in terms of query submission but differs in error message handling. While PQsendQuery resets the connection's error message before processing, PQsendQueryContinue preserves any existing error message in conn->errorMessage.

This function is primarily used internally within libpq when there's a need to send a query while maintaining the context of previous error conditions. It calls PQsendQueryInternal with the `false` parameter, indicating that error message reset should be skipped.

The function is not exported from the libpq library, making it an internal utility function for specialized error handling scenarios where preserving error context across multiple operations is important.

## Parameters / Member Variables
- `conn`: Pointer to the PostgreSQL connection object
- `query`: C string containing the SQL query to execute

## Dependencies
- Functions called/Symbols referenced:
  - [PQsendQueryInternal](PQsendQueryInternal.md) (internal implementation function)
- Called from (representative examples):
  - pgunlock_thread (threading utilities)

## Notes and Other Information
- Returns 1 on successful submission, 0 on error
- This is a non-exported function, internal to libpq implementation
- Does NOT reset conn->errorMessage before processing (key difference from PQsendQuery)
- Used for specialized internal scenarios where error context preservation is needed
- Shares the same core functionality as PQsendQuery through PQsendQueryInternal
- Part of libpq's internal error handling and threading infrastructure
- Allows for more sophisticated error reporting strategies in multi-step operations
- The query parameter must be a null-terminated C string
- Maintains asynchronous execution semantics like PQsendQuery