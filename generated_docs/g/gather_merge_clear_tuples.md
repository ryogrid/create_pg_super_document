# gather_merge_clear_tuples

## Location
src/backend/executor/nodeGatherMerge.c: 519 - 539

## Overview
Clears all tuple table slots and frees any unused pending tuples from worker tuple buffers to prevent memory leaks during GatherMerge operations.

## Definition
```c
static void gather_merge_clear_tuples(GatherMergeState *gm_state)
```

## Detailed Description
gather_merge_clear_tuples performs essential memory management for GatherMerge operations by cleaning up unused tuples and clearing tuple slots. The function operates on each worker's tuple buffer and associated slot:

1. **Tuple Buffer Cleanup**: For each worker's GMReaderTupleBuffer, it frees all unread tuples by iterating from the current readCounter to nTuples and calling pfree() on each MinimalTuple. This prevents memory leaks from tuples that were fetched from workers but never consumed.

2. **Slot Clearing**: Clears each worker's tuple table slot using ExecClearTuple, ensuring the slot is in an empty state and ready for reuse.

The function is critical for memory management during rescans and cleanup operations, ensuring that partially processed tuple batches don't accumulate and cause memory leaks.

## Parameters / Member Variables
- `gm_state`: The GatherMergeState containing worker tuple buffers and slots to be cleared

## Dependencies
- Functions called/Symbols referenced:
  - pfree
  - ExecClearTuple
- Called from (representative examples):
  - ExecReScanGatherMerge
  - gather_merge_getnext

## Notes and Other Information
- Only processes worker tuple buffers (indexed 0 to nreaders-1), not the leader slot
- The function carefully frees only unread tuples (from readCounter to nTuples) to avoid double-free errors
- Essential for preventing memory leaks during rescans when tuple buffers may contain partially processed batches
- Worker slots are indexed as i+1 in gm_slots array (slot 0 is reserved for leader)
- Called during both normal cleanup operations and rescan preparation to maintain clean state
- The pfree operations are safe because the tuples are MinimalTuples allocated with palloc