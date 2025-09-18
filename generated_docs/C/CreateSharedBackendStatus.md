# CreateSharedBackendStatus

## Location
[src/backend/utils/activity/backend_status.c:116-246](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/backend_status.c#L116-L246)

## Overview
Initializes the shared memory structures used for tracking backend status information during postmaster startup.

## Definition
```c
void CreateSharedBackendStatus(void)
```

## Detailed Description
This function creates and initializes all the shared memory structures required for the backend status system. It allocates space for the main backend status array, application name buffer, client hostname buffer, activity buffer, and conditionally SSL and GSS status buffers. When creating these structures for the first time, it zeros them out and sets up proper pointer relationships between the main status array and the various string buffers.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [mul_size](../m/mul_size.md)
  - [ShmemInitStruct](../S/ShmemInitStruct.md)
  - MemSet
  - [PgBackendStatus](../P/PgBackendStatus.md)
  - NumBackendStatSlots
  - NAMEDATALEN
  - pgstat_track_activity_query_size
  - BackendStatusArray
  - BackendAppnameBuffer
  - BackendClientHostnameBuffer
  - BackendActivityBuffer
  - [PgBackendSSLStatus](../P/PgBackendSSLStatus.md) (ifdef USE_SSL)
  - [PgBackendGSSStatus](../P/PgBackendGSSStatus.md) (ifdef ENABLE_GSS)
- Called from:
  - [CreateOrAttachShmemStructs](CreateOrAttachShmemStructs.md)

## Notes and Other Information
The function follows PostgreSQL's shared memory initialization pattern using ShmemInitStruct, which either creates new structures or attaches to existing ones in case of restart. It properly handles the conditional compilation of SSL and GSS features. The function ensures that all string pointers in the backend status array point to the correct locations in their respective shared buffers, enabling efficient access to backend information across processes.