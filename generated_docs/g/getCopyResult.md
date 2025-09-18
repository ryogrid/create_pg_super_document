# getCopyResult

## Location
src/interfaces/libpq/fe-exec.c: 2224 - 2261

## Overview
Internal helper function for PQgetResult that generates appropriate PGresult objects for COPY operations (COPY IN, COPY OUT, COPY BOTH).

## Definition


## Detailed Description
getCopyResult is a specialized helper function called by PQgetResult when the connection is in a COPY-related asynchronous state. The function handles three main scenarios for COPY operations.

First, it checks if the server connection has been lost by verifying conn->status against CONNECTION_OK. If the connection is bad, it saves the error state, resets the async status to IDLE, and returns an error result containing the connection error message.

If the connection is healthy, the function checks whether there's already an async result available that matches the requested copy type. If such a result exists, it returns it via pqPrepareAsyncResult.

Finally, if no existing result is available, the function creates a new empty PGresult with the appropriate copy status using PQmakeEmptyPGresult. This allows the application to begin or continue COPY operations.

## Parameters / Member Variables
- : Pointer to the PGconn structure representing the database connection
- : ExecStatusType indicating the specific type of COPY operation (PGRES_COPY_IN, PGRES_COPY_OUT, or PGRES_COPY_BOTH)

## Dependencies
- Functions called/Symbols referenced:
  - CONNECTION_OK
  - pqSaveErrorResult
  - PGASYNC_IDLE
  - pqPrepareAsyncResult
  - PQmakeEmptyPGresult
  - ExecStatusType
- Called from (representative examples):
  - PQgetResult (for COPY_IN, COPY_OUT, and COPY_BOTH cases)

## Notes and Other Information
- This is a static (internal) function not exposed in the public libpq API
- Handles error recovery when connections are lost during COPY operations
- Creates appropriate result objects to enable COPY protocol handling
- Essential for PostgreSQL's COPY FROM/TO functionality in libpq
- The function ensures that applications receive proper result objects even when no data has been transferred yet