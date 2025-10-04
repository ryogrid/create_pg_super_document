# PQcancelStatus

## Location
[src/interfaces/libpq/fe-cancel.c:284-294](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-cancel.c#L284-L294)

## Overview
Retrieves the current connection status of a cancel connection, providing insight into the state of the cancellation process.

## Definition
```c
ConnStatusType PQcancelStatus(const PGcancelConn *cancelConn)
```

## Detailed Description
PQcancelStatus is a simple wrapper function that returns the current connection status of a PGcancelConn structure. It provides the same status information as PQstatus() but specifically for cancel connections created with PQcancelCreate().

This function is essential for monitoring the progress of non-blocking cancellation operations initiated with PQcancelStart() and continued with PQcancelPoll(). The status values help determine the current phase of the cancellation process and whether any errors have occurred.

The function simply delegates to PQstatus() on the underlying connection structure, providing a consistent interface for status checking across both regular and cancel connections.

## Parameters / Member Variables
- `cancelConn`: Pointer to a const PGcancelConn structure whose status is to be checked

## Dependencies
- Functions called/Symbols referenced:
  - [PQstatus](PQstatus.md)
- Called from (representative examples):
  - [test_cancel](../t/test_cancel.md) (src/test/modules/libpq_pipeline/libpq_pipeline.c:342)
  - [test_cancel](../t/test_cancel.md) (src/test/modules/libpq_pipeline/libpq_pipeline.c:399)

## Notes and Other Information
- Returns ConnStatusType enum value indicating the current connection state
- Common status values include:
  - CONNECTION_ALLOCATED: Initial state after PQcancelCreate()
  - CONNECTION_STARTED: Connection establishment in progress
  - CONNECTION_AWAITING_RESPONSE: Waiting for server response to cancellation
  - CONNECTION_OK: Cancellation completed successfully
  - CONNECTION_BAD: Error occurred during cancellation process
- This function is read-only and does not modify the cancelConn state
- Can be called safely at any time after PQcancelCreate()
- Useful for debugging and monitoring cancellation progress in applications
- The const qualifier indicates this function does not modify the cancel connection

## Simplified Source

```c
ConnStatusType
PQcancelStatus(const PGcancelConn *cancelConn)
{
    // Simply return the status of the underlying connection
    return PQstatus(&cancelConn->conn);
}
```