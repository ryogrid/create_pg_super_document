# heap_prune_record_dead_or_unused

## Location
src/backend/access/heap/pruneheap.c: 1280 - 1296

## Overview
A decision function that records a line pointer as either dead or unused based on the pruning state configuration, providing flexibility in how dead tuples are handled during pruning.

## Definition


## Detailed Description
This function serves as a dispatcher that decides whether to mark a line pointer as LP_DEAD or LP_UNUSED based on the `mark_unused_now` flag in the pruning state. When `mark_unused_now` is true, dead tuples can be immediately removed during pruning by setting their line pointers to LP_UNUSED, which frees up space immediately. When false, the line pointers are marked as LP_DEAD, leaving the actual removal for a later VACUUM operation.

The function uses the `unlikely` hint to indicate that immediate unused marking is less common than deferring to VACUUM.

## Parameters / Member Variables
- `prstate`: Pointer to the PruneState structure tracking the current pruning operation
- `offnum`: The offset number of the line pointer to be marked dead or unused  
- `was_normal`: Boolean indicating whether the original line pointer pointed to a normal tuple

## Dependencies
- Functions called/Symbols referenced:
  - PruneState (structure)
  - heap_prune_record_unused
  - heap_prune_record_dead
- Called from (representative examples):
  - heap_prune_chain

## Notes and Other Information
- Acts as a conditional dispatcher between dead and unused marking strategies
- Uses `unlikely` compiler hint since immediate unused marking is less common
- Provides flexibility in pruning strategy - immediate cleanup vs deferred cleanup
- The `mark_unused_now` setting depends on various factors like HOT cleanup requirements
- Part of PostgreSQL's adaptive heap pruning mechanism that can optimize for different scenarios
- Allows for more aggressive space reclamation when conditions permit