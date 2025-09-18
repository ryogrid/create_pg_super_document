# RegisterSyncRequest

## Location
[src/backend/storage/sync/sync.c:580-619](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/sync/sync.c#L580-L619)

## Overview
RegisterSyncRequest is a function that handles file synchronization requests either locally (for standalone backends) or by forwarding them to the checkpointer process, providing a unified interface for managing file sync operations across different PostgreSQL process contexts.

## Definition
```c
bool RegisterSyncRequest(const FileTag *ftag, SyncRequestType type, bool retryOnError)
```

## Detailed Description
RegisterSyncRequest serves as the central entry point for registering file synchronization operations in PostgreSQL. The function operates in two distinct modes based on the process context:

1. **Local Mode**: When `pendingOps` is not NULL (indicating a standalone backend or startup process), sync requests are handled locally by calling `RememberSyncRequest()`.

2. **Checkpointer Mode**: In normal multi-process operation, sync requests are forwarded to the checkpointer process via `ForwardSyncRequest()`. If the request fails and `retryOnError` is true, the function enters a retry loop, sleeping for 10ms between attempts using `WaitLatch()`.

The function includes sophisticated error handling and retry logic to ensure critical sync operations (especially unlink requests) are not lost due to temporary queue congestion. The retry mechanism uses a latch-based wait with postmaster death detection for clean shutdown behavior.

## Parameters / Member Variables
- `ftag`: Pointer to a FileTag structure that identifies the specific file requiring synchronization
- `type`: SyncRequestType enum value specifying the type of sync operation (e.g., fsync, unlink)  
- `retryOnError`: Boolean flag controlling retry behavior when the sync request queue is full

## Dependencies
- Functions called/Symbols referenced:
  - [RememberSyncRequest](RememberSyncRequest.md): Handles local sync request storage
  - [ForwardSyncRequest](../F/ForwardSyncRequest.md): Forwards sync requests to checkpointer process
  - [WaitLatch](../W/WaitLatch.md): Provides timed wait with postmaster death detection
  - `SyncRequestType`: Enum defining sync operation types
  - `FileTag`: Structure identifying files for sync operations
  - `WL_EXIT_ON_PM_DEATH`: Latch wait option for postmaster death detection
  - `WL_TIMEOUT`: Latch wait option for timeout behavior

- Called from (representative examples):
  - [SlruPhysicalWritePage](../S/SlruPhysicalWritePage.md): SLRU page write operations
  - [SlruInternalDeleteSegment](../S/SlruInternalDeleteSegment.md): SLRU segment deletion
  - [register_dirty_segment](../r/register_dirty_segment.md): Dirty segment registration in md.c
  - [register_unlink_segment](../r/register_unlink_segment.md): File unlink operations in md.c
  - [register_forget_request](../r/register_forget_request.md): Sync request cancellation
  - [ForgetDatabaseSyncRequests](../F/ForgetDatabaseSyncRequests.md): Database-wide sync request cleanup

## Notes and Other Information
- The function includes a critical design decision to avoid CHECK_FOR_INTERRUPTS in the retry loop for SYNC_UNLINK_REQUEST operations, ensuring that file deletion requests are not lost due to query cancellation
- The 10ms retry interval represents a balance between responsiveness and system load when the checkpointer queue is congested
- The function always returns true for local operations (standalone/startup processes) since local sync state management cannot fail in the same way as inter-process communication
- Location: src/backend/storage/sync/sync.c:580-619