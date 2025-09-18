# TidStoreEndIterate

## Location
src/backend/access/common/tidstore.c: 536 - 550

## Overview
Finishes the iteration on a TidStore by cleaning up the TidStoreIter structure and freeing associated memory resources.

## Definition


## Detailed Description
This function completes the TidStore iteration process by properly terminating the underlying tree iterator and freeing all memory allocated for the TidStoreIter structure. It handles both shared and local TidStore variants by calling the appropriate cleanup function. The function releases the output buffer memory and the iterator structure itself.

This function is the required counterpart to TidStoreBeginIterate and must be called to prevent memory leaks.

## Parameters / Member Variables
- : The TidStoreIter structure to clean up and free

## Dependencies
- Functions called/Symbols referenced:
  - TidStoreIsShared
  - shared_ts_end_iterate
  - local_ts_end_iterate
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [lazy_vacuum_heap_rel](../l/lazy_vacuum_heap_rel.md)
  - [check_set_block_offsets](../c/check_set_block_offsets.md)

## Notes and Other Information
- Must be called for every TidStoreIter created by TidStoreBeginIterate to prevent memory leaks
- The caller remains responsible for releasing any locks held on the TidStore
- Automatically handles both shared and local TidStore cleanup
- Frees both the output buffer and the iterator structure itself
- Should be called even if iteration was not completed (e.g., due to early termination)