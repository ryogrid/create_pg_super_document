# heap_prune_record_unchanged_lp_redirect

## Location
src/backend/access/heap/pruneheap.c: 1536 - 1560

## Overview
Records a redirect line pointer (LP_REDIRECT) that remains unchanged during heap page pruning, marking it as processed without additional bookkeeping.

## Definition
```c
static void heap_prune_record_unchanged_lp_redirect(PruneState *prstate, OffsetNumber offnum)
```

## Detailed Description
This function handles redirect line pointers that don't need to be modified during the current pruning pass. Redirect line pointers are special entries that point to other line pointers on the same page, typically created during HOT (Heap-Only Tuple) updates.

The function's logic is deliberately minimal because:
1. **No Tuple Counting**: Redirect line pointers don't represent actual tuples, so they don't contribute to live tuple counts
2. **Separate Bookkeeping**: The actual tuple that the redirect points to will be processed separately with its own accounting
3. **Simple State Tracking**: Only needs to mark the line pointer as processed to ensure complete page coverage

This simplicity reflects the nature of redirect line pointers as metadata rather than actual data storage.

## Parameters / Member Variables
- `prstate`: Pointer to the pruning state structure that tracks which line pointers have been processed
- `offnum`: The offset number of the LP_REDIRECT line pointer being recorded

## Dependencies
- Functions called/Symbols referenced:
  - PruneState (structure for tracking pruning state)
  - Assert (debugging assertion macro)
- Called from (representative examples):
  - [heap_prune_chain](heap_prune_chain.md)

## Notes and Other Information
- This is a static function, only accessible within pruneheap.c
- Notably does not take a Page parameter unlike other similar functions, since redirect processing doesn't need to examine page content
- Part of PostgreSQL's HOT (Heap-Only Tuple) update mechanism support
- The most minimal of the heap_prune_record_unchanged_lp_* family of functions
- Redirect line pointers are created during UPDATE operations that don't require index updates
- The target of the redirect will be processed separately with appropriate tuple counting and visibility tracking