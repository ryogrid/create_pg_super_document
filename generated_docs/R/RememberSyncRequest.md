# RememberSyncRequest

## Location
[src/backend/storage/sync/sync.c:487-579](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/sync/sync.c#L487-L579)

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
  - [hash_search](../h/hash_search.md) (find, enter, or remove hash table entries)
  - [hash_seq_init](../h/hash_seq_init.md)/hash_seq_search (iterate through hash table for filter operations)
  - syncsw[].sync_filetagmatches (handler-specific file matching function)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (switch to pendingOpsCxt for allocations)
  - [palloc](../p/palloc.md) (allocate memory for unlink entries)
  - [lappend](../l/lappend.md) (add entries to pendingUnlinks list)
  - PendingFsyncEntry (structure for fsync requests)
  - [PendingUnlinkEntry](../P/PendingUnlinkEntry.md) (structure for unlink requests)
  - SyncRequestType (enum defining request types)
- Called from (representative examples):
  - [AbsorbSyncRequests](../A/AbsorbSyncRequests.md) (in checkpointer.c:1305)
  - [RegisterSyncRequest](RegisterSyncRequest.md) (in sync.c:588)

## Notes and Other Information
- Requires pendingOps hash table to be initialized (via InitSync)
- Preserves cycle_ctr for existing fsync entries to maintain proper chronological ordering
- Uses different data structures for different request types: hash table for fsyncs, linked list for unlinks
- Memory allocations are performed in pendingOpsCxt to ensure proper memory management
- SYNC_FILTER_REQUEST can cancel multiple requests matching a pattern, useful for operations affecting multiple files
- SYNC_FORGET_REQUEST cancels specific individual requests, typically used when operations are rolled back
- Critical for coordinating between backend processes (that generate sync requests) and the checkpointer (that processes them)
- The cycle counter mechanism ensures requests are processed in the correct checkpoint cycle even if the hash table is modified during processing

## Simplified Source

```c
// Simplified version of RememberSyncRequest
void RememberSyncRequest(const FileTag *ftag, SyncRequestType type) {
    // Validate that sync subsystem is initialized
    Assert(pendingOps);

    if (type == SYNC_FORGET_REQUEST) {
        // Cancel a specific previously entered fsync request
        PendingFsyncEntry *entry = hash_search(pendingOps, ftag, HASH_FIND, NULL);
        if (entry != NULL)
            entry->canceled = true;
    }
    else if (type == SYNC_FILTER_REQUEST) {
        // Cancel all fsync requests matching the file pattern
        HASH_SEQ_STATUS scan_status;
        PendingFsyncEntry *fsync_entry;

        hash_seq_init(&scan_status, pendingOps);
        while ((fsync_entry = hash_seq_search(&scan_status)) != NULL) {
            if (files_match(ftag, &fsync_entry->tag))
                fsync_entry->canceled = true;
        }

        // Cancel matching unlink requests too
        ListCell *cell;
        foreach(cell, pendingUnlinks) {
            PendingUnlinkEntry *unlink_entry = lfirst(cell);
            if (files_match(ftag, &unlink_entry->tag))
                unlink_entry->canceled = true;
        }
    }
    else if (type == SYNC_UNLINK_REQUEST) {
        // Add file deletion request to unlink list
        MemoryContext old_context = MemoryContextSwitchTo(pendingOpsCxt);

        PendingUnlinkEntry *entry = palloc(sizeof(PendingUnlinkEntry));
        entry->tag = *ftag;
        entry->cycle_ctr = checkpoint_cycle_ctr;
        entry->canceled = false;

        pendingUnlinks = lappend(pendingUnlinks, entry);
        MemoryContextSwitchTo(old_context);
    }
    else {
        // Normal case: add fsync request to hash table
        Assert(type == SYNC_REQUEST);

        MemoryContext old_context = MemoryContextSwitchTo(pendingOpsCxt);
        bool found;

        PendingFsyncEntry *entry = hash_search(pendingOps, ftag, HASH_ENTER, &found);

        // Initialize new entries or reactivate canceled ones
        if (!found || entry->canceled) {
            entry->cycle_ctr = sync_cycle_ctr;
            entry->canceled = false;
        }
        // Note: Keep existing cycle_ctr for already-active entries

        MemoryContextSwitchTo(old_context);
    }
}
```

Key simplifications made:
- Abstracted file matching logic into conceptual `files_match()` function
- Removed low-level hash table operation details
- Simplified memory context switching pattern
- Added clear comments for each request type's purpose
- Consolidated similar variable declarations
- Focused on the main logic flow rather than implementation details