# PQcancelStart

## Location
src/interfaces/libpq/fe-cancel.c: 186 - 207

## Overview
Initiates sending a cancellation request in a non-blocking fashion, beginning the asynchronous process of query cancellation.

## Definition
```c
int PQcancelStart(PGcancelConn *cancelConn)
```

## Detailed Description
PQcancelStart begins the non-blocking process of sending a query cancellation request to a PostgreSQL server. This function is the first step in the non-blocking cancellation workflow, which must be followed by repeated calls to PQcancelPoll() until the operation completes.

The function performs several validation checks:
1. Verifies that the cancelConn parameter is not NULL
2. Ensures the connection is not in a BAD state
3. Confirms the connection is in ALLOCATED state (not already being used for cancellation)

If all validation passes, it delegates to pqConnectDBStart() to begin the actual connection establishment process that will be used to send the cancellation request. The function is designed to return immediately without blocking, allowing the caller to continue other operations while the cancellation proceeds in the background.

## Parameters / Member Variables
- `cancelConn`: Pointer to a PGcancelConn structure created by PQcancelCreate() and ready for cancellation

## Dependencies
- Functions called/Symbols referenced:
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md)
  - [pqConnectDBStart](../p/pqConnectDBStart.md)
  - CONNECTION_BAD (status constant)
  - CONNECTION_ALLOCATED (status constant)
- Called from (representative examples):
  - [PQcancelBlocking](PQcancelBlocking.md) (src/interfaces/libpq/fe-cancel.c:174)
  - [libpqsrv_cancel](../l/libpqsrv_cancel.md) (src/include/libpq/libpq-be-fe-helpers.h:399)
  - [test_cancel](../t/test_cancel.md) (src/test/modules/libpq_pipeline/libpq_pipeline.c:296)

## Notes and Other Information
- Returns 1 if the cancellation process was successfully initiated, 0 on failure
- This is a non-blocking operation that returns immediately
- After calling this function, use PQcancelPoll() repeatedly to drive the cancellation to completion
- The connection status will be changed from CONNECTION_ALLOCATED to an appropriate state during processing
- Only one cancellation request can be active per PGcancelConn at a time
- Attempting to start cancellation on an already active cancelConn will result in an error