# AtEOXact_RelationMap

## Location
[src/backend/utils/cache/relmapper.c:541-587](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relmapper.c#L541-L587)

## Overview
Handles relation mapping cleanup and persistence at the end of transaction (commit or abort), including special handling for parallel workers.

## Definition

```c
void
AtEOXact_RelationMap(bool isCommit, bool isParallelWorker)
```
## Detailed Description
The AtEOXact_RelationMap function is called at the end of every transaction to handle relation mapping state changes. It behaves differently depending on whether the transaction is committing or aborting, and whether it's running in a parallel worker process.

**During Commit (non-parallel worker):**
- Asserts that no pending updates exist (all updates should have been activated via CommandCounterIncrement)
- Writes any active mapping updates to the actual map files on disk via perform_relmap_update
- Resets the active update counters to zero after successful persistence
- Ensures map changes are durably committed before transaction completion

**During Abort or in Parallel Worker:**
- Simply discards all pending and active mapping updates by resetting their counters to zero
- For parallel workers, asserts that no pending updates should exist (as they don't make mapping changes)
- Relies on normal post-abort cleanup to fix any affected relcache entries

The timing of this function during commit is critical - it must be called as late as possible before the actual transaction commit to minimize the window where the transaction could still roll back after committing map changes.

## Parameters / Member Variables
- `isCommit`: Boolean indicating whether the transaction is committing (true) or aborting (false)
- `isParallelWorker`: Boolean indicating whether this is being called from a parallel worker process
## Dependencies
- Functions called/Symbols referenced:
  - [perform_relmap_update](../p/perform_relmap_update.md) (called for both shared and local mappings during commit)
  - Assert (for debugging checks on pending updates)
- Global variables accessed:
  - active_shared_updates (static RelMapFile structure)
  - active_local_updates (static RelMapFile structure)
  - pending_shared_updates (static RelMapFile structure) 
  - pending_local_updates (static RelMapFile structure)
- Called from (representative examples):
  - [CommitTransaction](../C/CommitTransaction.md) (in src/backend/access/transam/xact.c)
  - [AbortTransaction](AbortTransaction.md) (in src/backend/access/transam/xact.c)

## Notes and Other Information
- This function is critical for maintaining consistency of the relation mapping system across transaction boundaries
- The timing during commit is carefully orchestrated to minimize the risk of inconsistency if the transaction rolls back after map changes are written
- Parallel workers have special handling since they receive mapping updates from the leader process but don't generate their own
- The function assumes that all pending updates have been properly activated via CommandCounterIncrement before commit
- Map file updates are protected by RelationMappingLock and include WAL logging for crash recovery

## Simplified Source

```c
// Simplified version of AtEOXact_RelationMap
void AtEOXact_RelationMap(bool isCommit, bool isParallelWorker) {
    if (isCommit && !isParallelWorker) {
        // During commit: persist active mapping changes to disk

        // Verify no pending updates exist (should all be activated)
        Assert(pending_shared_updates.num_mappings == 0);
        Assert(pending_local_updates.num_mappings == 0);

        // Write shared catalog mappings to file if any exist
        if (active_shared_updates.num_mappings != 0) {
            perform_relmap_update(true, &active_shared_updates);
            active_shared_updates.num_mappings = 0;
        }

        // Write local database mappings to file if any exist
        if (active_local_updates.num_mappings != 0) {
            perform_relmap_update(false, &active_local_updates);
            active_local_updates.num_mappings = 0;
        }
    } else {
        // During abort or parallel worker: discard all mapping changes

        // Parallel workers shouldn't have pending updates
        Assert(!isParallelWorker || pending_shared_updates.num_mappings == 0);
        Assert(!isParallelWorker || pending_local_updates.num_mappings == 0);

        // Reset all mapping update counters to discard changes
        active_shared_updates.num_mappings = 0;
        active_local_updates.num_mappings = 0;
        pending_shared_updates.num_mappings = 0;
        pending_local_updates.num_mappings = 0;
    }
}
```

Key simplifications made:
- Added descriptive comments explaining the main logic branches
- Clarified the purpose of each assertion check
- Grouped related operations with explanatory comments
- Simplified conditional logic flow with clear if/else structure
- Used more descriptive inline comments for each major operation
- Maintained all essential logic while improving readability