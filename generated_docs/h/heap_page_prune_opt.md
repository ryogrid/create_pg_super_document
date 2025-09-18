# heap_page_prune_opt

## Location
src/backend/access/heap/pruneheap.c: 193 - 349

## Overview
heap_page_prune_opt is an opportunistic page maintenance function that performs housekeeping on heap pages by pruning dead tuples and repairing fragmentation when certain heuristics indicate the page would benefit from cleanup.

## Definition
void heap_page_prune_opt(Relation relation, Buffer buffer)

## Detailed Description
This function implements an opportunistic approach to heap page maintenance that balances performance with cleanup effectiveness. It only performs pruning when heuristics suggest the page is a good candidate and when it can acquire the necessary cleanup lock without blocking. The function performs several key checks:

1. **Recovery Mode Check**: Skips processing during recovery since WAL cannot be written
2. **Prune Transaction ID Validation**: Only proceeds if the page has a valid prune_xid indicating potential dead tuples
3. **Visibility Test**: Uses global visibility state to determine if the prune_xid represents removable transactions
4. **Free Space Heuristics**: Evaluates whether the page has insufficient free space based on the relation's fill factor target

When conditions are met, it attempts to acquire an exclusive buffer cleanup lock conditionally (non-blocking) and performs the actual pruning via heap_page_prune_and_freeze. The function updates PostgreSQL statistics to track the number of reclaimed tuples, carefully accounting for newly dead items versus actually freed space.

## Parameters / Member Variables
- : The heap relation containing the page to be pruned
- : Buffer containing the page to be potentially pruned (caller must have pin but not lock)

## Dependencies
- Functions called/Symbols referenced:
  - BufferGetPage
  - RecoveryInProgress
  - TransactionIdIsValid
  - GlobalVisTestFor
  - GlobalVisTestIsRemovableXid
  - RelationGetTargetPageFreeSpace
  - PageIsFull
  - PageGetHeapFreeSpace
  - ConditionalLockBufferForCleanup
  - heap_page_prune_and_freeze
  - pgstat_update_heap_dead_tuples
  - LockBuffer
- Called from (representative examples):
  - heap_prepare_pagescan
  - heapam_index_fetch_tuple
  - heapam_scan_bitmap_next_block

## Notes and Other Information
- This is a frequently called function, designed to exit quickly when pruning is not beneficial
- Uses non-blocking lock acquisition to avoid performance degradation
- Free space calculations are done without locks for performance, accepting potential minor inaccuracies
- Does not update the Free Space Map (FSM) to encourage reuse of freed space by updates to the same page
- Carefully tracks statistics to distinguish between newly dead items and actually reclaimed space
- The mark_unused_now parameter is currently always false for safety during on-access pruning