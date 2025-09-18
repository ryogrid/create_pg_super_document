# ExecIndexOnlyScanEstimate

## Location
src/backend/executor/nodeIndexonlyscan.c: 706 - 725

## Overview
ExecIndexOnlyScanEstimate computes the shared memory space required for parallel index-only scan operations and registers these requirements with the parallel query dynamic shared memory (DSM) estimator.

## Definition
```c
void ExecIndexOnlyScanEstimate(IndexOnlyScanState *node, ParallelContext *pcxt)
```

## Detailed Description
This function is part of PostgreSQL's parallel query infrastructure and specifically handles memory estimation for parallel index-only scan operations. It calculates the amount of shared memory needed to coordinate parallel index-only scanning across multiple worker processes.

The function operates by calling the lower-level index_parallelscan_estimate function, which determines the space requirements based on the specific index access method, the number of scan keys, the number of ORDER BY keys, and the current snapshot. The estimated space is then registered with the parallel context's shared memory table of contents (TOC) estimator.

The estimation process considers the index relation descriptor, scan keys for filtering, ORDER BY keys for sorting, and the transaction snapshot for visibility determination. This information allows the parallel query coordinator to allocate sufficient shared memory for all worker processes to coordinate their scanning activities effectively.

## Parameters / Member Variables
- `node`: Pointer to the IndexOnlyScanState containing scan configuration and state information needed for estimation
- `pcxt`: Pointer to the ParallelContext containing the shared memory estimator and coordination structures

## Dependencies
- Functions called/Symbols referenced:
  - [index_parallelscan_estimate](../i/index_parallelscan_estimate.md)
  - shm_toc_estimate_chunk
  - shm_toc_estimate_keys
- Types used:
  - [IndexOnlyScanState](../I/IndexOnlyScanState.md)
  - [ParallelContext](../P/ParallelContext.md)
  - [EState](EState.md)
- Called from (representative examples):
  - ExecParallelEstimate

## Notes and Other Information
- This function is essential for parallel query execution planning and shared memory allocation
- The estimated length is stored in the node's ioss_PscanLen field for later use during parallel scan initialization
- The function registers both the memory chunk size and the number of keys needed in the shared memory TOC
- Parallel index-only scans can significantly improve query performance on large datasets by distributing the workload across multiple worker processes
- The estimation must account for index-specific coordination requirements that vary between different index access methods
- Proper memory estimation is critical for avoiding shared memory allocation failures during parallel query execution