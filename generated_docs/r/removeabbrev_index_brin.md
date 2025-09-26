# removeabbrev_index_brin

## Location
[src/backend/utils/sort/tuplesortvariants.c:1711-1724](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesortvariants.c#L1711-L1724)

## Overview
Removes abbreviated keys from BRIN tuple sorting data structures and restores the original datum values used for comparison.

## Definition

```c
static void
removeabbrev_index_brin(Tuplesortstate *state, SortTuple *stups, int count)
```
## Detailed Description
This function is part of PostgreSQL's BRIN (Block Range Index) tuple sorting implementation. It operates on an array of SortTuple structures that contain abbreviated keys for performance optimization during sorting. When abbreviated keys can no longer be used (typically due to insufficient discrimination between values), this function removes the abbreviated representations and restores the original datum values (bt_blkno - block numbers) that will be used for tuple comparison instead.

The function iterates through each SortTuple in the provided array, extracts the underlying BrinSortTuple, and sets the datum1 field to the original block number (bt_blkno) from the BRIN tuple. This ensures that subsequent comparisons will use the actual block number values rather than the abbreviated keys.

## Parameters / Member Variables
- : Pointer to the Tuplesortstate structure managing the sort operation
- : Array of SortTuple structures containing the tuples to process
- : Number of tuples in the stups array to process

## Dependencies
- Functions called/Symbols referenced:
  - [Tuplesortstate](../T/Tuplesortstate.md) (sort state management structure)
  - SortTuple (generic sort tuple structure)
  - [BrinSortTuple](../B/BrinSortTuple.md) (BRIN-specific tuple structure)
- Called from (representative examples):
  - [tuplesort_begin_index_brin](../t/tuplesort_begin_index_brin.md) (BRIN sort initialization)
  - CLUSTER_SORT (clustering sort operations)

## Notes and Other Information
- This function is specific to BRIN index tuple sorting and is part of the abbreviated key optimization system
- The bt_blkno field represents the block number associated with the BRIN tuple, which is the primary sort key for BRIN index operations
- This function is typically called when the sort algorithm determines that abbreviated keys are not providing sufficient performance benefits
- The function modifies the SortTuple structures in-place, updating their datum1 fields with the original comparison values