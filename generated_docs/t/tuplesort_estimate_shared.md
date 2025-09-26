# tuplesort_estimate_shared

## Location
[src/backend/utils/sort/tuplesort.c:2955-2975](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesort.c#L2955-L2975)

## Overview
Estimates the amount of shared memory required for parallel tuple sorting operations based on the number of worker processes.

## Definition

```c
Size
tuplesort_estimate_shared(int nWorkers)
```
## Detailed Description
The `tuplesort_estimate_shared` function calculates the shared memory space needed to support parallel tuple sorting with a given number of worker processes. It computes the memory requirements for the shared tape system that enables coordination between multiple worker processes during parallel sort operations. The calculation includes space for `TapeShare` structures (one per worker) plus the base `Sharedsort` structure, with proper memory alignment considerations.

This function is essential for parallel query planning, allowing PostgreSQL to determine if sufficient shared memory is available before attempting to launch parallel sort operations. It's used during the initialization phase of parallel index builds and other operations that require coordinated sorting across multiple processes.

## Parameters / Member Variables
- `nWorkers`: The estimated number of worker processes that will participate in the parallel sort operation (must be > 0)

## Dependencies
- Functions called/Symbols referenced:
  - [mul_size](../m/mul_size.md) (safe multiplication function to prevent overflow)
  - [TapeShare](../T/TapeShare.md) (structure type for shared tape coordination)
  - [add_size](../a/add_size.md) (safe addition function to prevent overflow)  
  - [Sharedsort](../S/Sharedsort.md) (structure type for shared sort state)
- Called from (representative examples):
  - [_brin_begin_parallel](../b/_brin_begin_parallel.md) (src/backend/access/brin/brin.c:2402)
  - [_bt_begin_parallel](../b/_bt_begin_parallel.md) (src/backend/access/nbtree/nbtsort.c:1446)

## Notes and Other Information
- This is a public function (non-static), accessible from other compilation units as declared in tuplesort.h
- The function ensures proper MAXALIGN alignment for the BufFile shared state to meet platform alignment requirements
- Uses PostgreSQL's safe arithmetic functions (mul_size, add_size) to prevent integer overflow attacks
- The memory estimation is conservative and includes alignment padding
- Part of PostgreSQL's parallel sorting infrastructure introduced for parallel index builds
- The returned size represents the total shared memory segment size needed, not per-worker memory requirements
- Used primarily during the planning phase of parallel operations to ensure resource availability