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

## Simplified Source

```c
// Simplified version of CreateSharedBackendStatus
void CreateSharedBackendStatus(void) {
    Size size;
    bool found;
    int i;
    char *buffer;

    // Create main backend status array
    size = mul_size(sizeof(PgBackendStatus), NumBackendStatSlots);
    BackendStatusArray = (PgBackendStatus *)
        ShmemInitStruct("Backend Status Array", size, &found);

    if (!found) {
        MemSet(BackendStatusArray, 0, size);
    }

    // Create application name buffer and link pointers
    size = mul_size(NAMEDATALEN, NumBackendStatSlots);
    BackendAppnameBuffer = (char *)
        ShmemInitStruct("Backend Application Name Buffer", size, &found);

    if (!found) {
        MemSet(BackendAppnameBuffer, 0, size);
        // Link each backend status entry to its appname buffer slot
        buffer = BackendAppnameBuffer;
        for (i = 0; i < NumBackendStatSlots; i++) {
            BackendStatusArray[i].st_appname = buffer;
            buffer += NAMEDATALEN;
        }
    }

    // Create client hostname buffer and link pointers
    size = mul_size(NAMEDATALEN, NumBackendStatSlots);
    BackendClientHostnameBuffer = (char *)
        ShmemInitStruct("Backend Client Host Name Buffer", size, &found);

    if (!found) {
        MemSet(BackendClientHostnameBuffer, 0, size);
        // Link each backend status entry to its hostname buffer slot
        buffer = BackendClientHostnameBuffer;
        for (i = 0; i < NumBackendStatSlots; i++) {
            BackendStatusArray[i].st_clienthostname = buffer;
            buffer += NAMEDATALEN;
        }
    }

    // Create activity buffer for query text and link pointers
    BackendActivityBufferSize = mul_size(pgstat_track_activity_query_size, NumBackendStatSlots);
    BackendActivityBuffer = (char *)
        ShmemInitStruct("Backend Activity Buffer", BackendActivityBufferSize, &found);

    if (!found) {
        MemSet(BackendActivityBuffer, 0, BackendActivityBufferSize);
        // Link each backend status entry to its activity buffer slot
        buffer = BackendActivityBuffer;
        for (i = 0; i < NumBackendStatSlots; i++) {
            BackendStatusArray[i].st_activity_raw = buffer;
            buffer += pgstat_track_activity_query_size;
        }
    }

#ifdef USE_SSL
    // Create SSL status buffer if SSL support is enabled
    size = mul_size(sizeof(PgBackendSSLStatus), NumBackendStatSlots);
    BackendSslStatusBuffer = (PgBackendSSLStatus *)
        ShmemInitStruct("Backend SSL Status Buffer", size, &found);

    if (!found) {
        MemSet(BackendSslStatusBuffer, 0, size);
        // Link SSL status pointers
        for (i = 0; i < NumBackendStatSlots; i++) {
            BackendStatusArray[i].st_sslstatus = &BackendSslStatusBuffer[i];
        }
    }
#endif

#ifdef ENABLE_GSS
    // Create GSS status buffer if GSS support is enabled
    size = mul_size(sizeof(PgBackendGSSStatus), NumBackendStatSlots);
    BackendGssStatusBuffer = (PgBackendGSSStatus *)
        ShmemInitStruct("Backend GSS Status Buffer", size, &found);

    if (!found) {
        MemSet(BackendGssStatusBuffer, 0, size);
        // Link GSS status pointers
        for (i = 0; i < NumBackendStatSlots; i++) {
            BackendStatusArray[i].st_gssstatus = &BackendGssStatusBuffer[i];
        }
    }
#endif
}
```

Key simplifications made:
- Consolidated repetitive buffer creation patterns into clear sections
- Simplified pointer arithmetic by using array indexing where clearer
- Added descriptive comments for each major operation
- Maintained the essential logic flow and all critical functionality
- Preserved conditional compilation directives for SSL and GSS features
- Focused on the main execution path while keeping error handling implicit