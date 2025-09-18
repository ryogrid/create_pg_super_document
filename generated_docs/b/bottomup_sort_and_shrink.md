# bottomup_sort_and_shrink

## Location
src/backend/access/heap/heapam.c: 8653 - 8781

## Overview
A helper function for heap_index_delete_tuples() that sorts and optimizes the deltids array for bottom-up deletion processing, applying sophisticated heuristics to maximize deletion efficiency.

## Definition
```c
static int bottomup_sort_and_shrink(TM_IndexDeleteOp *delstate)
```

## Detailed Description
This function performs comprehensive optimization of the deletion array for bottom-up index deletion processing. It groups heap TIDs by block, applies power-of-two bucketing to normalize promising TID counts, sorts blocks by deletion potential, and shrinks the array to focus on the most promising blocks.

Key processing steps:

1. **Block Grouping**: Groups TIDs from deltids by heap block number, calculating per-block statistics (ntids, npromisingtids)

2. **Power-of-Two Bucketing**: Normalizes npromisingtids values using power-of-two rounding (minimum 4) to reduce noise and enable locality-based tie-breaking

3. **Multi-Level Sorting**: Applies sophisticated sorting via bottomup_sort_and_shrink_cmp():
   - Primary: npromisingtids (descending - most promising first)
   - Secondary: ntids (descending, with power-of-two bucketing)
   - Tertiary: heap block number (ascending - spatial locality)

4. **Array Shrinking**: Limits processing to BOTTOMUP_MAX_NBLOCKS most promising blocks, often reducing array size significantly

5. **Reordering**: Reconstructs deltids array in optimal processing order

The power-of-two bucketing scheme is crucial for balancing deletion efficiency with spatial locality, treating small differences in promising TID counts as noise while preserving meaningful distinctions.

## Parameters / Member Variables
- `delstate`: Pointer to TM_IndexDeleteOp structure containing the deltids array to optimize and related state

## Dependencies
- Functions called/Symbols referenced:
  - bottomup_sort_and_shrink_cmp
  - bottomup_nblocksfavorable  
  - qsort
  - palloc, pfree, memcpy
  - pg_nextpower2_32
  - ItemPointerGetBlockNumber
  - BlockNumberIsValid
  - Min
  - IndexDeleteCounts, TM_IndexDelete, TM_IndexStatus (structure types)
  - BOTTOMUP_MAX_NBLOCKS (constant)
- Called from (representative examples):
  - heap_index_delete_tuples

## Notes and Other Information
- Assumes input deltids array is already sorted in TID order
- Returns number of "favorable" blocks (contiguous/nearly-contiguous blocks at start of processing order)
- Uses power-of-two bucketing to ignore small differences in npromisingtids (treated as noise)
- Handles npromisingtids ≤ 4 specially by rounding up to 4
- Often shrinks deltids array to small fraction of original size by focusing on most promising blocks
- The bucketing scheme enables heap locality factors to influence processing order without sacrificing deletion efficiency
- Allocates temporary arrays (blockgroups, reordereddeltids) that are freed before return
- Located in src/backend/access/heap/heapam.c:8653-8781