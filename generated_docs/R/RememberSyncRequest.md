# RememberSyncRequest

## Location
src/backend/storage/sync/sync.c: 487 - 579

## Overview
Processes synchronization requests from backends by adding them to appropriate data structures (hash table for fsync requests, linked list for unlink requests) or canceling existing requests.

## Definition
```c
void RememberSyncRequest(const FileTag *ftag, SyncRequestType type)
```

## Detailed Description
RememberSyncRequest serves as the callback function from the checkpointer side of the sync request mechanism. It handles four different types of sync requests: SYNC_REQUEST (normal fsync requests), SYNC_UNLINK_REQUEST (file deletion requests), SYNC_FORGET_REQUEST (cancel specific fsync requests), and SYNC_FILTER_REQUEST (cancel multiple matching requests). For fsync requests, entries are stored in the pendingOps hash table with cycle counters to track when they were submitted. Unlink requests go into a separate pendingUnlinks linked list for processing during SyncPostCheckpoint. The function carefully preserves the oldest cycle counter for existing entries to ensure proper ordering during processing.

The function operates within the checkpointer's memory context and includes logic to handle request cancellation, which is essential for operations like file truncation or deletion that need to invalidate previously queued sync requests for the affected files.

## Parameters / Member Variables
- `ftag`: Pointer to FileTag structure identifying the file and operation
- `type`: SyncRequestType enum specifying the kind of sync request (SYNC_REQUEST, SYNC_UNLINK_REQUEST, SYNC_FORGET_REQUEST, or SYNC_FILTER_REQUEST)

## Dependencies
- Functions called/Symbols referenced:
  - hash_search (find, enter, or remove hash table entries)
  - hash_seq_init/hash_seq_search (iterate through hash table for filter operations)
  - syncsw[].sync_filetagmatches (handler-specific file matching function)
  - MemoryContextSwitchTo (switch to pendingOpsCxt for allocations)
  - palloc (allocate memory for unlink entries)
  - lappend (add entries to pendingUnlinks list)
  - PendingFsyncEntry (structure for fsync requests)
  - PendingUnlinkEntry (structure for unlink requests)
  - SyncRequestType (enum defining request types)
- Called from (representative examples):
  - AbsorbSyncRequests (in checkpointer.c:1305)
  - RegisterSyncRequest (in sync.c:588)

## Notes and Other Information
- Requires pendingOps hash table to be initialized (via InitSync)
- Preserves cycle_ctr for existing fsync entries to maintain proper chronological ordering
- Uses different data structures for different request types: hash table for fsyncs, linked list for unlinks
- Memory allocations are performed in pendingOpsCxt to ensure proper memory management
- SYNC_FILTER_REQUEST can cancel multiple requests matching a pattern, useful for operations affecting multiple files
- SYNC_FORGET_REQUEST cancels specific individual requests, typically used when operations are rolled back
- Critical for coordinating between backend processes (that generate sync requests) and the checkpointer (that processes them)
- The cycle counter mechanism ensures requests are processed in the correct checkpoint cycle even if the hash table is modified during processing