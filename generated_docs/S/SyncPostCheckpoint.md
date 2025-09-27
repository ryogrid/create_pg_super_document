# SyncPostCheckpoint

## Location
[src/backend/storage/sync/sync.c:202-285](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/sync/sync.c#L202-L285)

## Overview
Performs post-checkpoint cleanup by safely removing files that were marked for deletion in previous checkpoint cycles, ensuring they can be unlinked without compromising database consistency.

## Definition
```c
void SyncPostCheckpoint(void)
```

## Detailed Description
SyncPostCheckpoint is called after a checkpoint completes successfully to perform deferred file deletion operations. It iterates through the pendingUnlinks list and removes files that were marked for deletion in previous checkpoint cycles (identified by having a cycle_ctr less than the current checkpoint_cycle_ctr). The function includes important safety mechanisms: it skips canceled entries, handles race conditions with DROP DATABASE operations gracefully by ignoring ENOENT errors, and periodically calls AbsorbSyncRequests() to prevent blocking fsync request processing during lengthy deletion operations.

The function implements cycle-based deletion to ensure files are not removed until it's safe to do so - specifically, until the checkpoint that recorded the deletion request has completed successfully. This prevents corruption scenarios where files are deleted before the transaction log adequately reflects the database state.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [AbsorbSyncRequests](../A/AbsorbSyncRequests.md) (periodically absorb incoming sync requests during processing)
  - UNLINKS_PER_ABSORB (constant defining how often to absorb sync requests)
  - [PendingUnlinkEntry](../P/PendingUnlinkEntry.md) (structure representing files to be deleted)
  - syncsw[].sync_unlinkfiletag (handler-specific file deletion function)
  - [list_free_deep](../l/list_free_deep.md) (free entire list and its contents)
  - [list_cell_number](../l/list_cell_number.md) (get position of cell in list)
  - [list_nth](../l/list_nth.md) (get nth element of list)
  - [list_delete_first_n](../l/list_delete_first_n.md) (remove first n elements from list)
- Called from (representative examples):
  - [CreateCheckPoint](../C/CreateCheckPoint.md) (main checkpoint creation function in xlog.c:7288)

## Notes and Other Information
- Only processes entries from previous checkpoint cycles (cycle_ctr < checkpoint_cycle_ctr) to ensure safe deletion timing
- Handles race conditions with DROP DATABASE by ignoring ENOENT errors when files are already deleted
- Marks processed entries as canceled as a safety measure
- Implements periodic absorption of sync requests (every UNLINKS_PER_ABSORB deletions) to prevent blocking other operations
- Efficiently manages memory by either freeing the entire list or removing only processed entries
- Includes protection against cycle counter wraparound, which would only delay deletion by one checkpoint cycle
- Critical for maintaining database consistency by ensuring files are only deleted after their removal is safely recorded in the transaction log

## Simplified Source

```c
// Simplified version of SyncPostCheckpoint
void SyncPostCheckpoint(void) {
    int absorb_counter = UNLINKS_PER_ABSORB;
    ListCell *lc;

    // Iterate through all pending file deletion requests
    foreach(lc, pendingUnlinks) {
        PendingUnlinkEntry *entry = (PendingUnlinkEntry *) lfirst(lc);
        char path[MAXPGPATH];

        // Skip canceled entries
        if (entry->canceled)
            continue;

        // Only process entries from previous checkpoint cycles
        // This ensures files are safe to delete
        if (entry->cycle_ctr == checkpoint_cycle_ctr)
            break;

        // Attempt to delete the file
        if (syncsw[entry->tag.handler].sync_unlinkfiletag(&entry->tag, path) < 0) {
            // Handle race condition with DROP DATABASE
            if (errno != ENOENT) {
                ereport(WARNING,
                       (errcode_for_file_access(),
                        errmsg("could not remove file \"%s\": %m", path)));
            }
        }

        // Mark entry as processed
        entry->canceled = true;

        // Periodically absorb sync requests to avoid blocking
        if (--absorb_counter <= 0) {
            AbsorbSyncRequests();
            absorb_counter = UNLINKS_PER_ABSORB;
        }
    }

    // Clean up the processed entries from the list
    if (lc == NULL) {
        // Processed all entries - free entire list
        list_free_deep(pendingUnlinks);
        pendingUnlinks = NIL;
    } else {
        // Remove only the processed entries
        int entries_to_delete = list_cell_number(pendingUnlinks, lc);

        for (int i = 0; i < entries_to_delete; i++)
            pfree(list_nth(pendingUnlinks, i));

        pendingUnlinks = list_delete_first_n(pendingUnlinks, entries_to_delete);
    }
}
```

Key simplifications made:
- Removed detailed comments about cycle counter wraparound and race conditions
- Condensed the error handling logic while preserving the ENOENT check
- Simplified the list cleanup logic explanation
- Focused on the main execution flow: iterate, check, delete, cleanup
- Preserved all essential safety mechanisms and error handling