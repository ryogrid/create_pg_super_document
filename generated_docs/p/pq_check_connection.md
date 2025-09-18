# pq_check_connection

## Location
[src/backend/libpq/pqcomm.c:2053-2084](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqcomm.c#L2053-L2084)

## Overview
Checks if the client connection is still active by polling for socket closure events without blocking, returning false if the connection has been lost.

## Definition
```c
bool pq_check_connection(void)
```

## Detailed Description
This function performs a non-blocking check to determine whether the client connection is still alive. It uses PostgreSQL's WaitEventSet infrastructure to poll for socket closure events on the frontend-backend connection. The function modifies the wait event filter to specifically look for WL_SOCKET_CLOSED events and performs a zero-timeout wait to immediately check the socket status without blocking.

If a latch event is detected during polling, it resets the latch and retries the check, as latch events might mask other events. This ensures accurate detection of connection state even when other events are pending.

The function is primarily used by the interrupt processing system to detect lost connections during long-running operations, allowing PostgreSQL to abort processing when the client is no longer available.

## Parameters / Member Variables
- No parameters (void function)
- Returns: `true` if the connection is still active, `false` if the connection has been closed

## Dependencies
- Functions called/Symbols referenced:
  - [ModifyWaitEvent](../M/ModifyWaitEvent.md)
  - WaitEventSetWait
  - [ResetLatch](../R/ResetLatch.md)
  - FeBeWaitSet (global wait event set)
  - FeBeWaitSetSocketPos (socket position in wait set)
  - MyLatch (current process latch)
  - WL_SOCKET_CLOSED (wait event flag)
  - WL_LATCH_SET (wait event flag)
  - FeBeWaitSetNEvents (maximum events constant)
  - [WaitEvent](../W/WaitEvent.md) (event structure type)
  - lengthof (macro for array length)
- Called from (representative examples):
  - ProcessInterrupts (in postgres.c:3332)

## Notes and Other Information
- Uses a zero timeout (non-blocking) approach to check connection status immediately
- Modifies the global FeBeWaitSet without restoring it, following the pattern used by other wait sites
- Handles latch events by resetting and retrying to ensure other events aren't masked
- Part of PostgreSQL's client connection monitoring system controlled by client_connection_check_interval
- The function is typically called during CHECK_FOR_INTERRUPTS() processing
- Does not wake up idle sessions and is specifically designed for active query processing contexts
- Critical for detecting client disconnections during long-running operations to prevent resource waste