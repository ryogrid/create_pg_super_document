# PQcancelPoll

## Location
[src/interfaces/libpq/fe-cancel.c:208-283](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-cancel.c#L208-L283)

## Overview
Polls a cancel connection to advance the non-blocking cancellation process, handling the server response and determining completion status.

## Definition
```c
PostgresPollingStatusType PQcancelPoll(PGcancelConn *cancelConn)
```

## Detailed Description
PQcancelPoll continues the non-blocking cancellation process started by PQcancelStart(). This function must be called repeatedly until it returns either PGRES_POLLING_OK (success) or PGRES_POLLING_FAILED (failure).

The function operates in two phases:
1. **Connection Establishment Phase**: While the connection status is not CONNECTION_AWAITING_RESPONSE, it delegates to PQconnectPoll() to handle the standard connection establishment process.
2. **Response Waiting Phase**: Once CONNECTION_AWAITING_RESPONSE is reached, it waits for the server to close the connection, which indicates that the cancellation request has been processed.

During the response waiting phase, the function:
- Attempts to read data from the connection using pqReadData()
- Returns PGRES_POLLING_READING if no data is available yet
- Considers unexpected data reception as an error condition
- Treats connection closure (EOF) as successful cancellation completion
- Handles platform-specific EOF behavior differences (especially on Windows)

## Parameters / Member Variables
- `cancelConn`: Pointer to a PGcancelConn structure with an active cancellation request

## Dependencies
- Functions called/Symbols referenced:
  - [PQconnectPoll](PQconnectPoll.md)
  - [pqReadData](../p/pqReadData.md)
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md)
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md)
  - CONNECTION_AWAITING_RESPONSE
  - CONNECTION_OK
  - CONNECTION_BAD
  - PGRES_POLLING_READING
  - PGRES_POLLING_OK
  - PGRES_POLLING_FAILED
- Called from (representative examples):
  - [libpqsrv_cancel](../l/libpqsrv_cancel.md) (src/include/libpq/libpq-be-fe-helpers.h:412)
  - [pqConnectDBComplete](../p/pqConnectDBComplete.md) (src/interfaces/libpq/fe-connect.c:2562)
  - [test_cancel](../t/test_cancel.md) (src/test/modules/libpq_pipeline/libpq_pipeline.c:303)

## Notes and Other Information
- Returns PostgresPollingStatusType indicating current status:
  - PGRES_POLLING_OK: Cancellation completed successfully
  - PGRES_POLLING_FAILED: Cancellation failed
  - PGRES_POLLING_READING: Still waiting for server response, call again
- The server communicates successful cancellation by closing the connection (EOF)
- Windows platforms have special EOF handling due to TCP connection closure behavior differences
- Any unexpected data received from the server is treated as an error condition
- The function expects only connection closure, not data exchange with the server
- Error conditions result in CONNECTION_BAD status and detailed error messages