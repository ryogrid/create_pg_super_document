# ExecIndexScanEstimate

## Location
[src/backend/executor/nodeIndexscan.c:1641-1660](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeIndexscan.c#L1641-L1660)

## Overview
ExecIndexScanEstimate computes the amount of shared memory space needed for a parallel index scan and informs the parallel context estimator about the requirements.

## Definition

```c
void
ExecIndexScanEstimate(IndexScanState *node,
					  ParallelContext *pcxt)
```
## Detailed Description
ExecIndexScanEstimate is part of PostgreSQL's parallel scan support infrastructure. It calculates the shared memory requirements for coordinating a parallel index scan across multiple worker processes. The function delegates to the index access method's index_parallelscan_estimate function to determine the specific memory needs based on the index type, scan keys, and ordering requirements.

The function performs two key operations:
1. Calls index_parallelscan_estimate to calculate the required shared memory size for the parallel scan descriptor, considering the index relation, number of scan keys, number of order-by keys, and the current snapshot
2. Updates the parallel context estimator with the memory requirements using shm_toc_estimate_chunk and shm_toc_estimate_keys

The calculated size is stored in the IndexScanState's iss_PscanLen field for later use during parallel scan initialization.

## Parameters / Member Variables
- : Pointer to IndexScanState containing the index scan execution state with information about the index, scan keys, and order-by keys
- : Pointer to ParallelContext that tracks shared memory requirements for parallel query execution

## Dependencies
- Functions called/Symbols referenced:
  - [index_parallelscan_estimate](../i/index_parallelscan_estimate.md)
  - shm_toc_estimate_chunk
  - shm_toc_estimate_keys
- Called from (representative examples):
  - ExecParallelEstimate (in execParallel.c:246)

## Notes and Other Information
- This function is part of the parallel scan support framework introduced for parallel query execution
- The actual memory allocation and initialization is handled by ExecIndexScanInitializeDSM
- The estimation depends on the specific index access method's parallel scan capabilities
- The function assumes the IndexScanState has been properly initialized with scan keys and order-by keys
- The shared memory is used to coordinate scan progress between parallel workers
- Located in src/backend/executor/nodeIndexscan.c:1641-1660