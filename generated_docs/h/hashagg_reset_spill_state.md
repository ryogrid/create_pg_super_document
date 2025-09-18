# hashagg_reset_spill_state

## Location
src/backend/executor/nodeAgg.c: 3133 - 3172

## Overview
Frees all resources and memory allocated for spilled hash aggregation operations during cleanup or reset scenarios.

## Definition
```c
static void hashagg_reset_spill_state(AggState *aggstate)
```

## Detailed Description
This function performs comprehensive cleanup of all spill-related resources used during hash aggregation when disk spilling was necessary. It systematically deallocates memory and closes file handles in three main areas:

1. **Spill structures cleanup**: Frees the initial spill data structures including ntuples arrays and partitions arrays for each grouping set, then deallocates the main hash_spills array
2. **Batch cleanup**: Uses deep list freeing to deallocate all HashAggBatch structures that were created from spilled partitions
3. **Tape set cleanup**: Closes the logical tape set used for disk I/O operations during spilling

This function ensures no memory leaks occur when hash aggregation operations are completed or need to be reset.

## Parameters / Member Variables
- `aggstate`: The aggregate execution state containing all spill-related resources to be freed

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md)
  - [list_free_deep](../l/list_free_deep.md)
  - LogicalTapeSetClose
- Types used:
  - [AggState](../A/AggState.md)
  - [HashAggSpill](../H/HashAggSpill.md)
- Constants used:
  - NIL
- Called from (representative examples):
  - [ExecEndAgg](../E/ExecEndAgg.md) (src/backend/executor/nodeAgg.c:4334)
  - [ExecReScanAgg](../E/ExecReScanAgg.md) (src/backend/executor/nodeAgg.c:4450)

## Notes and Other Information
- This is a static function internal to nodeAgg.c
- Safe to call multiple times or when no spilling occurred (checks for NULL pointers)
- Part of PostgreSQL's resource management for disk-based hash aggregation
- Called during both normal cleanup (ExecEndAgg) and rescan operations (ExecReScanAgg)
- Does not free the hash tables themselves, only spill-related resources
- Sets all freed pointers to NULL to prevent double-free errors
- Essential for preventing memory leaks in long-running queries with multiple aggregation phases