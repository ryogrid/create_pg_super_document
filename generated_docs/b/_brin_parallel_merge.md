# _brin_parallel_merge

## Location
src/backend/access/brin/brin.c: 2610 - 2756

## Overview
Coordinates the final merge phase of parallel BRIN index building by collecting and merging sorted results from all worker processes into the complete index.

## Definition
```c
static double _brin_parallel_merge(BrinBuildState *state)
```

## Detailed Description
This function orchestrates the final phase of parallel BRIN index building by:

1. **Wait for workers**: First waits for all parallel workers to complete their heap scanning using _brin_parallel_heapscan()
2. **Sort results**: Performs the final sort of all BRIN tuples collected from workers, ordered by block number
3. **Merge overlapping ranges**: Combines BRIN tuples that represent the same page range but were produced by different workers using union operations
4. **Fill gaps**: Inserts empty BRIN summaries for page ranges that had no tuples during the parallel scan
5. **Build final index**: Inserts all merged and gap-filled BRIN tuples into the actual index

The function handles the complex logic of merging potentially overlapping page range summaries from multiple workers while maintaining the block number ordering essential for BRIN index efficiency.

## Parameters / Member Variables
- `state`: Pointer to BrinBuildState containing all necessary build context including the shared tuplesort state, index relation, and build parameters

## Dependencies
- Functions called/Symbols referenced:
  - [_brin_parallel_heapscan](_brin_parallel_heapscan.md) (wait for worker completion)
  - tuplesort_performsort (sort collected tuples)
  - [tuplesort_getbrintuple](../t/tuplesort_getbrintuple.md) (retrieve sorted tuples)
  - tuplesort_end (cleanup sort state)
  - [brin_new_memtuple](brin_new_memtuple.md) (create memory tuple)
  - [brin_deform_tuple](brin_deform_tuple.md) (convert tuple to memory format)
  - [brin_form_tuple](brin_form_tuple.md) (convert memory tuple to disk format)
  - [union_tuples](../u/union_tuples.md) (merge overlapping range summaries)
  - [brin_doinsert](brin_doinsert.md) (insert tuple into index)
  - [brin_fill_empty_ranges](brin_fill_empty_ranges.md) (fill gaps with empty summaries)
  - AllocSetContextCreate/MemoryContextReset/MemoryContextDelete (memory management)
- Called from (representative examples):
  - [brinbuild](brinbuild.md) (main BRIN index build function)

## Notes and Other Information
- Returns the total number of heap tuples scanned across all workers
- Uses a separate memory context for union operations to prevent memory bloat
- Handles three scenarios for each tuple: first tuple, same range as previous, or new range
- Ensures all page ranges are represented in the index, filling empty ranges as needed
- The merge process is optimized for mostly non-overlapping ranges (typical case)
- Union operations may be expensive, so memory context is reset periodically
- Maintains block number ordering which is crucial for BRIN index scan performance