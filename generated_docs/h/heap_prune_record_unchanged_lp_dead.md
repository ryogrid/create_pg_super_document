# heap_prune_record_unchanged_lp_dead

## Location
[src/backend/access/heap/pruneheap.c:1508-1535](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/pruneheap.c#L1508-L1535)

## Overview
Records a line pointer that was already LP_DEAD and remains unchanged during heap page pruning, tracking it for later vacuum processing.

## Definition
```c
static void heap_prune_record_unchanged_lp_dead(Page page, PruneState *prstate, OffsetNumber offnum)
```

## Detailed Description
This function handles line pointers that are already marked as LP_DEAD (dead) and don't need modification during the current pruning pass. The function's main responsibilities are:

1. **State Tracking**: Marks the line pointer as processed in the pruning state
2. **Dead Offset Recording**: Adds the offset to the dead offsets array for later vacuum processing
3. **Page State Management**: Deliberately avoids setting hastup flag to allow for potential page truncation optimization

The function implements an important optimization assumption: LP_DEAD items encountered during pruning will likely become LP_UNUSED before the vacuum process completes. This assumption enables more frequent relation truncation during VACUUM operations.

## Parameters / Member Variables
- `page`: The heap page containing the dead line pointer
- `prstate`: Pointer to the pruning state structure that tracks pruning progress
- `offnum`: The offset number of the LP_DEAD line pointer being recorded

## Dependencies
- Functions called/Symbols referenced:
  - PruneState (structure for tracking pruning state)
  - Assert (debugging assertion macro)
- Called from (representative examples):
  - [heap_page_prune_and_freeze](heap_page_prune_and_freeze.md)

## Notes and Other Information
- This is a static function, only accessible within pruneheap.c
- The function deliberately doesn't set the hastup flag for LP_DEAD items, making an optimization assumption about future LP_UNUSED conversion
- Does not immediately unset all_visible flag; this is deferred to heap_page_prune_and_freeze() to allow freezing attempts
- Part of PostgreSQL's strategy to optimize relation truncation by treating LP_DEAD items as provisional
- The dead offsets are collected for later processing by the vacuum machinery
- Critical for maintaining the correctness of VACUUM's page processing and space reclamation

## Simplified Source

```c
static void heap_prune_record_unchanged_lp_dead(Page page, PruneState *prstate, OffsetNumber offnum)
{
    // Mark this line pointer as processed
    prstate->processed[offnum] = true;

    // Record the dead offset for vacuum to handle later
    prstate->deadoffsets[prstate->lpdead_items++] = offnum;

    // Note: We deliberately don't set hastup flag here to allow
    // potential page truncation optimization during vacuum
}
```