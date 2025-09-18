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
  - list_delete_first_n (remove first n elements from list)
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