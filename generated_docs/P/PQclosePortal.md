# PQclosePortal

## Location
[src/interfaces/libpq/fe-exec.c:2539-2555](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L2539-L2555)

## Overview
PQclosePortal closes a previously created portal in PostgreSQL, similar to PQclosePrepared but specifically for portals created through SQL DECLARE CURSOR commands.

## Definition


## Detailed Description
PQclosePortal provides a synchronous interface to close a portal that was previously created on the PostgreSQL server. While libpq doesn't directly expose portals to client applications in most cases, this function can be used to close portals created by SQL DECLARE CURSOR commands. The function follows the standard libpq execution pattern by calling PQexecStart, sending the close command via PQsendTypedCommand, and completing the operation with PQexecFinish.

The function sends a Close message ('C') with portal type ('P') to the PostgreSQL backend to request closure of the specified portal. This is a blocking/synchronous operation that waits for the server response before returning.

## Parameters / Member Variables
- `conn`: Connection handle to the PostgreSQL database server
- `portal`: Name of the portal to close (null-terminated string)

## Dependencies
- Functions called/Symbols referenced:
  - [PQexecStart](PQexecStart.md)
  - [PQsendTypedCommand](PQsendTypedCommand.md)  
  - [PQexecFinish](PQexecFinish.md)
  - PqMsg_Close
- Called from (representative examples):
  - [test_prepared](../t/test_prepared.md) (in libpq_pipeline test module)

## Notes and Other Information
- Returns a PGresult object that should be freed with PQclear when no longer needed
- The function will return NULL if PQexecStart fails, indicating the connection is not ready for command execution
- This is the synchronous counterpart to PQsendClosePortal
- Portals are typically created implicitly by DECLARE CURSOR statements rather than directly through libpq
- The portal name must match exactly with a portal that exists on the server