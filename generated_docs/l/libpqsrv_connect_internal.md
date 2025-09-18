# libpqsrv_connect_internal

## Location
[src/include/libpq/libpq-be-fe-helpers.h:160-255](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/libpq/libpq-be-fe-helpers.h#L160-L255)

## Overview
Internal helper function that completes the asynchronous PostgreSQL connection establishment process with proper interrupt handling and resource management.

## Definition
```c
static inline void libpqsrv_connect_internal(PGconn *conn, uint32 wait_event_info)
```

## Detailed Description
libpqsrv_connect_internal implements the completion phase of asynchronous PostgreSQL connection establishment. It handles the complex polling loop required for non-blocking connections while ensuring proper interrupt processing and resource cleanup. The function manages several critical aspects: file descriptor resource management for failed connections, socket polling with platform-specific handling (especially for Windows), interrupt processing during potentially long connection phases, and exception safety with automatic resource cleanup.

The function uses PostgreSQL's latch-based waiting mechanism to efficiently wait for socket readiness while remaining responsive to interrupts and shutdown signals. It includes comprehensive error handling with PG_TRY/PG_CATCH blocks to ensure that file descriptors and connection resources are properly released even if exceptions occur during the connection process.

## Parameters / Member Variables
- `conn`: PostgreSQL connection handle returned by PQconnectStart* functions, or NULL
- `wait_event_info`: Event identifier used for wait event reporting during socket polling

## Dependencies
- Functions called/Symbols referenced:
  - ReleaseExternalFD
  - PQstatus
  - [PQsocket](../P/PQsocket.md)
  - [PQconnectPoll](../P/PQconnectPoll.md)
  - [PQfinish](../P/PQfinish.md)
  - [WaitLatchOrSocket](../W/WaitLatchOrSocket.md)
  - [ResetLatch](../R/ResetLatch.md)
  - CHECK_FOR_INTERRUPTS
- Called from (representative examples):
  - [libpqsrv_connect](libpqsrv_connect.md)
  - [libpqsrv_connect_params](libpqsrv_connect_params.md)

## Notes and Other Information
- This is a static inline function defined in src/include/libpq/libpq-be-fe-helpers.h:160-255
- Part of the internal helper functions section of the libpqsrv suite  
- Handles NULL connections by immediately releasing the reserved file descriptor
- Includes platform-specific code for Windows vs Unix socket handling during connection establishment
- Uses PostgreSQL's exception handling mechanism (PG_TRY/PG_CATCH) for robust resource management
- Processes interrupts during connection establishment to maintain server responsiveness
- The polling loop continues until connection succeeds (PGRES_POLLING_OK) or fails (PGRES_POLLING_FAILED)
- Automatically handles transitions between different polling states (READING/WRITING/CONNECTED)