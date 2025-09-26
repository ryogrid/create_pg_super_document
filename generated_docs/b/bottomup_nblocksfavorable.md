# bottomup_nblocksfavorable

## Location
[src/backend/access/heap/heapam.c:8537-8579](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L8537-L8579)

## Overview
Determines how many blocks should be considered favorable/contiguous for a bottom-up index deletion pass, optimizing heap block access patterns for spatial and temporal locality.

## Definition
```c
static int bottomup_nblocksfavorable(IndexDeleteCounts *blockgroups, int nblockgroups, TM_IndexDelete *deltids)
```

## Detailed Description
This function analyzes block groups to determine how many consecutive heap blocks can be processed favorably during bottom-up index deletion. It applies sophisticated heuristics to recognize workloads where heap blocks can be accessed contiguously or nearly contiguously, optimizing for both spatial and temporal locality.

The function examines blockgroups (which describe the final sort order for deltids) and identifies sequences of blocks that are physically close to each other on disk. It uses a tolerance mechanism (BOTTOMUP_TOLERANCE_NBLOCKS) to handle minor ordering variations that occur when nearly-contiguous blocks fall into different buckets due to small differences in promising TID counts.

Key design goals:
- Enable recognition of naturally occurring contiguous access patterns
- Optimize for workloads with heap block locality (e.g., skewed updates on low-cardinality indexes)
- Support both spatial locality (nearby blocks) and temporal locality (deterministic access patterns)
- Handle power-of-two bucketing schemes that create opportunities for batched contiguous processing

## Parameters / Member Variables
- `blockgroups`: Array of IndexDeleteCounts describing the final sort order for bottom-up deletion processing
- `nblockgroups`: Number of elements in the blockgroups array
- `deltids`: Array of TM_IndexDelete structures (used to interpret blockgroups, may not be sorted yet)

## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)
  - [IndexDeleteCounts](../I/IndexDeleteCounts.md) (structure type)
  - [TM_IndexDelete](../T/TM_IndexDelete.md) (structure type)  
  - BOTTOMUP_MAX_NBLOCKS (constant)
  - BOTTOMUP_TOLERANCE_NBLOCKS (constant)
- Called from (representative examples):
  - [bottomup_sort_and_shrink](bottomup_sort_and_shrink.md)

## Notes and Other Information
- Always returns at least 1 favorable block (degenerate case of single block)
- Uses tolerance mechanism to handle small blips in physical block ordering
- Optimized for naturally occurring access patterns in PostgreSQL workloads
- Particularly effective for low-cardinality indexes subject to skewed, non-HOT updates
- The tolerance value is described as "a little arbitrary, but works well enough in practice"
- Enables temporal locality when multiple indexes access the same heap blocks in similar patterns
- Located in src/backend/access/heap/heapam.c:8537-8579