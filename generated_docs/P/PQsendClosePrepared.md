# PQsendClosePrepared

## Location
[src/interfaces/libpq/fe-exec.c:2556-2568](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L2556-L2568)

## Overview
PQsendClosePrepared submits a Close Statement command to the PostgreSQL server asynchronously without waiting for completion, allowing for non-blocking closure of prepared statements.

## Definition
```c
int PQsendClosePrepared(PGconn *conn, const char *stmt)
```

## Detailed Description
PQsendClosePrepared provides an asynchronous interface to close a prepared statement on the PostgreSQL server. Unlike the synchronous PQclosePrepared function, this function sends the close command and returns immediately without waiting for the server's response. This allows applications to continue processing while the server handles the close operation.

The function sends a Close message ('C') with statement type ('S') to the PostgreSQL backend. Applications using this function must subsequently call PQgetResult to retrieve the result and complete the operation properly.

## Parameters / Member Variables
- `conn`: Connection handle to the PostgreSQL database server
- `stmt`: Name of the prepared statement to close (null-terminated string)

## Dependencies
- Functions called/Symbols referenced:
  - [PQsendTypedCommand](PQsendTypedCommand.md)
  - PqMsg_Close
- Called from (representative examples):
  - [test_prepared](../t/test_prepared.md) (in libpq_pipeline test module)

## Notes and Other Information
- Returns 1 if successfully submitted, 0 if error occurred (conn->errorMessage will be set)
- This is the asynchronous counterpart to PQclosePrepared
- Applications must call PQgetResult after this function to complete the operation
- The prepared statement name must match exactly with a statement that exists on the server
- Part of libpq's asynchronous command interface for non-blocking database operations

## Simplified Source

```c
int PQsendClosePrepared(PGconn *conn, const char *stmt) {
    // Send Close command for prepared statement ('S' type)
    return PQsendTypedCommand(conn, PqMsg_Close, 'S', stmt);
}
```