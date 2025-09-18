# ExecSeqScanEstimate

## Location
src/backend/executor/nodeSeqscan.c: 238 - 255

## Overview
ExecSeqScanEstimate computes the amount of shared memory space needed for parallel sequential scans in PostgreSQL's parallel query execution framework.

## Definition
```c
void ExecSeqScanEstimate(SeqScanState *node, ParallelContext *pcxt)
```

## Detailed Description
This function is part of PostgreSQL's parallel scan support infrastructure. It estimates the shared memory requirements for a parallel sequential scan by calling table_parallelscan_estimate() to determine the space needed for the parallel scan descriptor. The function then registers this memory requirement with the parallel context's shared memory table-of-contents (TOC) estimator, reserving both the memory chunk and a key for accessing it.

## Parameters / Member Variables
- `node`: A pointer to the SeqScanState structure containing the sequential scan state information
- `pcxt`: A pointer to the ParallelContext structure that manages parallel execution context and shared memory allocation

## Dependencies
- Functions called/Symbols referenced:
  - [table_parallelscan_estimate](../t/table_parallelscan_estimate.md)
  - shm_toc_estimate_chunk
  - shm_toc_estimate_keys
- Types referenced:
  - [SeqScanState](../S/SeqScanState.md)
  - [ParallelContext](../P/ParallelContext.md)
  - [EState](EState.md)
- Called from (representative examples):
  - ExecParallelEstimate (in execParallel.c)

## Notes and Other Information
- This function is part of the parallel scan support infrastructure in PostgreSQL
- It operates during the planning phase of parallel query execution to estimate memory requirements
- The function accesses the current relation and snapshot from the node's state
- The estimated length is stored in node->pscan_len for later use
- Located in src/backend/executor/nodeSeqscan.c at lines 238-255