# heap_prune_record_dead

## Location
src/backend/access/heap/pruneheap.c: 1246 - 1279

## Overview
Records a line pointer that should be marked as dead during heap pruning, tracking offsets of tuples that are no longer visible to any transactions.

## Definition


## Detailed Description
This function records that a line pointer at the specified offset should be marked as dead during the heap pruning operation. Dead tuples are those that are no longer visible to any active transaction and can potentially be removed by VACUUM. The function updates multiple tracking arrays in the pruning state to handle the dead tuple appropriately, including maintaining statistics and preparing information for future vacuum operations.

The function deliberately delays unsetting the all_visible flag to allow dead tuples that are removable to not prevent page freezing operations.

## Parameters / Member Variables
- `prstate`: Pointer to the PruneState structure tracking the current pruning operation
- `offnum`: The offset number of the line pointer to be marked dead
- `was_normal`: Boolean indicating whether the original line pointer pointed to a normal tuple (as opposed to a redirect)

## Dependencies
- Functions called/Symbols referenced:
  - PruneState (structure)
  - MaxHeapTuplesPerPage (constant)
- Called from (representative examples):
  - [heap_prune_record_dead_or_unused](heap_prune_record_dead_or_unused.md)

## Notes and Other Information
- Marks the offset as processed to prevent duplicate processing
- Maintains the `nowdead` array for immediate dead tuple tracking
- Records the offset in `deadoffsets` array for vacuum to later process
- Only counts deletions when marking a normal tuple as dead (not redirects)
- Deliberately delays unsetting all_visible flag to allow freezing of pages with removable dead tuples
- Part of PostgreSQL's heap pruning and HOT cleanup mechanism
- Works in coordination with VACUUM for complete dead tuple cleanup