# PQsendClosePortal

## Location
[src/interfaces/libpq/fe-exec.c:2569-2588](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L2569-L2588)

## Overview
PQsendClosePortal submits a Close Portal command to the PostgreSQL server asynchronously without waiting for completion, allowing for non-blocking closure of portals.

## Definition
```c
int PQsendClosePortal(PGconn *conn, const char *portal)
```

## Detailed Description
PQsendClosePortal provides an asynchronous interface to close a portal on the PostgreSQL server. Unlike the synchronous PQclosePortal function, this function sends the close command and returns immediately without waiting for the server's response. This allows applications to continue processing while the server handles the close operation.

The function sends a Close message ('C') with portal type ('P') to the PostgreSQL backend. Applications using this function must subsequently call PQgetResult to retrieve the result and complete the operation properly. This is particularly useful for portals created by SQL DECLARE CURSOR commands.

## Parameters / Member Variables
- `conn`: Connection handle to the PostgreSQL database server
- `portal`: Name of the portal to close (null-terminated string)

## Dependencies
- Functions called/Symbols referenced:
  - [PQsendTypedCommand](PQsendTypedCommand.md)
  - PqMsg_Close
- Called from (representative examples):
  - [test_prepared](../t/test_prepared.md) (in libpq_pipeline test module)

## Notes and Other Information
- Returns 1 if successfully submitted, 0 if error occurred (conn->errorMessage will be set)
- This is the asynchronous counterpart to PQclosePortal
- Applications must call PQgetResult after this function to complete the operation
- The portal name must match exactly with a portal that exists on the server
- Part of libpq's asynchronous command interface for non-blocking database operations
- Portals are typically created implicitly by DECLARE CURSOR statements rather than directly through libpq