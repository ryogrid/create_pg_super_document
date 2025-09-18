# heap_prune_record_redirect

## Location
src/backend/access/heap/pruneheap.c: 1215 - 1245

## Overview
Records a line pointer that should be converted to a redirect pointer during heap pruning, tracking the source and target offsets for the redirect operation.

## Definition


## Detailed Description
This function records that a line pointer at offset `offnum` should be converted to a redirect pointer pointing to offset `rdoffnum`. Redirects are used in PostgreSQL's HOT (Heap-Only Tuples) implementation to chain together versions of the same logical tuple that reside on the same page. The function updates the pruning state to track this redirect operation, which will be applied later during the actual page modification phase.

The function also maintains statistics about the pruning operation, including counting deleted tuples when a normal tuple is being redirected.

## Parameters / Member Variables
- `prstate`: Pointer to the PruneState structure tracking the current pruning operation
- `offnum`: The offset number of the line pointer to be redirected (source)
- `rdoffnum`: The offset number that the redirect should point to (target)
- `was_normal`: Boolean indicating whether the original line pointer pointed to a normal tuple

## Dependencies
- Functions called/Symbols referenced:
  - PruneState (structure)
  - MaxHeapTuplesPerPage (constant)
- Called from (representative examples):
  - heap_prune_chain

## Notes and Other Information
- Marks the source offset as processed to avoid double-processing
- Does not mark the redirect target as processed - it needs separate handling
- Maintains an array of redirect pairs in the pruning state
- Only counts deletions when redirecting a normal tuple (not when changing existing redirects)
- Sets the `hastup` flag to indicate the page contains tuples after pruning
- Part of PostgreSQL's HOT chain pruning mechanism for efficient tuple updates