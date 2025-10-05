# _brin_parallel_estimate_shared

## Location
[src/backend/access/brin/brin.c:2757-2767](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin.c#L2757-L2767)

## Overview
This function estimates the shared memory size required for parallel BRIN index building, calculating the total memory needed to store both the BRIN parallel build state and the parallel table scan state.

## Definition

```c
static Size
_brin_parallel_estimate_shared(Relation heap, Snapshot snapshot)
```
## Detailed Description
The function calculates the total shared memory requirement for a parallel BRIN index build operation. It combines the size needed for the BrinShared structure (which contains parallel build coordination data) with the memory required for the parallel table scan operation. The memory layout uses BUFFERALIGN to ensure proper alignment, following the same pattern used in shared memory table of contents (shm_toc) allocation.

## Parameters / Member Variables
- `heap`: The relation (table) for which the BRIN index is being built
- `snapshot`: The snapshot that will be used during the parallel scan operation
## Dependencies
- Functions called/Symbols referenced:
  - [add_size](../a/add_size.md): Safely adds two Size values, checking for overflow
  - BUFFERALIGN: Macro for aligning memory to buffer boundaries
  - [table_parallelscan_estimate](../t/table_parallelscan_estimate.md): Estimates memory needed for parallel table scanning
  - [BrinShared](../B/BrinShared.md): Structure containing shared state for parallel BRIN builds

- Called from (representative examples):
  - [_brin_begin_parallel](_brin_begin_parallel.md): Uses this estimate when setting up parallel BRIN builds

## Notes and Other Information
- This is a static function, only accessible within the brin.c file
- The BUFFERALIGN usage follows PostgreSQL's shared memory alignment conventions to ensure proper memory layout
- The estimation is crucial for allocating sufficient shared memory before starting the parallel build process
- The function combines two distinct memory requirements: BRIN-specific coordination data and generic parallel scan state

## Simplified Source

```c
static Size _brin_parallel_estimate_shared(Relation heap, Snapshot snapshot) {
    // Calculate total shared memory needed:
    // 1. BrinShared structure (properly aligned)
    // 2. Parallel table scan state
    return add_size(BUFFERALIGN(sizeof(BrinShared)),
                   table_parallelscan_estimate(heap, snapshot));
}
```