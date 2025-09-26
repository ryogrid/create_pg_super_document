# ExecIndexOnlyScanInitializeDSM

## Location
[src/backend/executor/nodeIndexonlyscan.c:726-763](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeIndexonlyscan.c#L726-L763)

## Overview
Initializes a dynamic shared memory (DSM) structure for parallel index-only scans, setting up the necessary shared memory resources and scan descriptors for worker processes to participate in parallel execution.

## Definition
```c
void ExecIndexOnlyScanInitializeDSM(IndexOnlyScanState *node, ParallelContext *pcxt)
```

## Detailed Description
This function is responsible for setting up the shared memory infrastructure required for parallel index-only scans. It allocates shared memory space for the parallel index scan descriptor, initializes it using the index parallel scan infrastructure, and creates an index scan descriptor that can be used by parallel worker processes. The function also handles the setup of scan keys if they are ready at initialization time.

The function performs several key operations:
1. Allocates shared memory for the parallel index scan descriptor
2. Initializes the parallel scan using the existing relation and index information
3. Inserts the descriptor into the shared memory table of contents for worker access
4. Creates a parallel-capable index scan descriptor
5. Configures the scan for index-only access by setting xs_want_itup
6. Optionally starts the scan if runtime keys are already available

## Parameters / Member Variables
- `node`: IndexOnlyScanState containing the executor state for the index-only scan node, including relation descriptors, scan keys, and runtime configuration
- `pcxt`: ParallelContext providing the shared memory infrastructure and coordination mechanisms for parallel execution

## Dependencies
- Functions called/Symbols referenced:
  - [shm_toc_allocate](../s/shm_toc_allocate.md)
  - [index_parallelscan_initialize](../i/index_parallelscan_initialize.md)  
  - [shm_toc_insert](../s/shm_toc_insert.md)
  - [index_beginscan_parallel](../i/index_beginscan_parallel.md)
  - [index_rescan](../i/index_rescan.md)
- Types used:
  - [IndexOnlyScanState](../I/IndexOnlyScanState.md)
  - [ParallelContext](../P/ParallelContext.md)
  - [ParallelIndexScanDesc](../P/ParallelIndexScanDesc.md)
- Called from (representative examples):
  - [ExecParallelInitializeDSM](ExecParallelInitializeDSM.md)

## Notes and Other Information
- This function is part of PostgreSQL's parallel query execution framework specifically for index-only scans
- The allocated shared memory size is determined by ioss_PscanLen which should be pre-calculated
- The function sets xs_want_itup to true to indicate that index tuples should be returned rather than heap tuples
- Runtime keys handling is deferred - the scan is only started immediately if runtime keys are not needed or already computed
- The InvalidBuffer assignment to ioss_VMBuffer indicates that visibility map buffer management will be handled separately for each worker