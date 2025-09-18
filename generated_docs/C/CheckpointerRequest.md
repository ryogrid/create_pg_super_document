# CheckpointerRequest

## Location
src/backend/postmaster/checkpointer.c: 106 - 125

## Overview
CheckpointerRequest is a structure that represents a single sync request submitted to the checkpointer process for file synchronization operations.

## Definition


## Detailed Description
The CheckpointerRequest structure encapsulates information about a file synchronization request that needs to be processed by the checkpointer background process. Each request contains the type of sync operation to perform and identifies the specific file through a FileTag. These requests are queued in the CheckpointerShmemStruct and processed during checkpoint operations to ensure data durability.

The checkpointer uses these requests to manage pending sync operations that need to be completed at the next checkpoint, helping to coordinate file system synchronization across PostgreSQL's storage subsystem.

## Parameters / Member Variables
- : A SyncRequestType enum value indicating the specific type of synchronization operation (sync or unlink)
- : A FileTag structure that uniquely identifies the file or relation segment to be synchronized

## Dependencies
- Functions called/Symbols referenced:
  - SyncRequestType
  - FileTag
- Used by:
  - [CheckpointerShmemStruct](CheckpointerShmemStruct.md) (as array element)
  - [ForwardSyncRequest](../F/ForwardSyncRequest.md)
  - [CheckpointerSlotMapping](CheckpointerSlotMapping.md)
  - [AbsorbSyncRequests](../A/AbsorbSyncRequests.md)

## Notes and Other Information
- [CheckpointerRequest](CheckpointerRequest.md) structures are stored in a flexible array member within CheckpointerShmemStruct
- The requests are processed during checkpoint operations to ensure file system consistency
- This structure is part of PostgreSQL's crash recovery and durability mechanisms
- The type field determines whether the operation is a sync (ensure data is written to disk) or unlink (remove file) operation