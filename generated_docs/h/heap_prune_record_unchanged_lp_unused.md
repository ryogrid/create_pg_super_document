# heap_prune_record_unchanged_lp_unused

## Location
src/backend/access/heap/pruneheap.c: 1319 - 1329

## Overview
Records an unused line pointer that remains unchanged during heap page pruning, marking it as processed in the pruning state.

## Definition


## Detailed Description
This function is part of the heap page pruning mechanism in PostgreSQL. It handles unused line pointers that do not need to be modified during the pruning process. The function's primary responsibility is to mark the line pointer at the specified offset as processed in the pruning state, ensuring that the pruning algorithm correctly tracks which line pointers have been handled.

The function is called when an unused line pointer is encountered that should remain in its current state. This is important for maintaining the integrity of the pruning process and ensuring that all line pointers on a page are accounted for.

## Parameters / Member Variables
- : The heap page being pruned
- : Pointer to the pruning state structure that tracks the pruning operation progress
- : The offset number of the unused line pointer being recorded

## Dependencies
- Functions called/Symbols referenced:
  - PruneState (structure used to track pruning state)
  - Assert (debugging assertion macro)
- Called from (representative examples):
  - [heap_page_prune_and_freeze](heap_page_prune_and_freeze.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the pruneheap.c file
- The function includes an assertion to ensure the line pointer hasn't already been processed
- Part of PostgreSQL's vacuum and pruning subsystem for managing dead tuples and reclaiming space
- The function is simple but critical for maintaining the correctness of the pruning state tracking