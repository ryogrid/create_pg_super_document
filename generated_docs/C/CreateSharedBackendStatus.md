# CreateSharedBackendStatus

## Location
src/backend/utils/activity/backend_status.c: 116 - 246

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
  - mul_size
  - ShmemInitStruct
  - MemSet
  - PgBackendStatus
  - NumBackendStatSlots
  - NAMEDATALEN
  - pgstat_track_activity_query_size
  - BackendStatusArray
  - BackendAppnameBuffer
  - BackendClientHostnameBuffer
  - BackendActivityBuffer
  - PgBackendSSLStatus (ifdef USE_SSL)
  - PgBackendGSSStatus (ifdef ENABLE_GSS)
- Called from:
  - CreateOrAttachShmemStructs

## Notes and Other Information
The function follows PostgreSQL's shared memory initialization pattern using ShmemInitStruct, which either creates new structures or attaches to existing ones in case of restart. It properly handles the conditional compilation of SSL and GSS features. The function ensures that all string pointers in the backend status array point to the correct locations in their respective shared buffers, enabling efficient access to backend information across processes.