# ExecIndexScanInitializeDSM

## Location
src/backend/executor/nodeIndexscan.c: 1661 - 1696

## Overview
ExecIndexScanInitializeDSM sets up a parallel index scan descriptor in shared memory, initializing the necessary data structures for coordinated parallel index scanning across multiple worker processes.

## Definition


## Detailed Description
ExecIndexScanInitializeDSM is responsible for initializing the shared memory structures needed for parallel index scanning. This function is called during the initialization phase of parallel query execution to set up coordination mechanisms between the leader process and worker processes.

The function performs the following key operations:
1. Allocates shared memory space using the size previously calculated by ExecIndexScanEstimate
2. Initializes the parallel index scan descriptor using index_parallelscan_initialize with the base relation, index relation, and current snapshot
3. Inserts the parallel scan descriptor into the shared memory table of contents for worker processes to find
4. Begins the parallel index scan using index_beginscan_parallel
5. If runtime keys are not needed or are already computed, immediately calls index_rescan to start the scan with the appropriate scan keys

This function ensures that all parallel workers can coordinate their scanning efforts and avoid duplicate work while maintaining consistency through the shared snapshot.

## Parameters / Member Variables
- : Pointer to IndexScanState containing the index scan execution state, scan keys, and previously calculated shared memory size
- : Pointer to ParallelContext containing the shared memory table of contents and coordination structures

## Dependencies
- Functions called/Symbols referenced:
  - shm_toc_allocate
  - index_parallelscan_initialize
  - shm_toc_insert
  - index_beginscan_parallel
  - index_rescan
- Called from (representative examples):
  - ExecParallelInitializeDSM (in execParallel.c:470)

## Notes and Other Information
- This function works in conjunction with ExecIndexScanEstimate which calculates the required shared memory size
- The parallel index scan descriptor is identified in shared memory by the plan node ID
- Runtime key handling is deferred until all keys are ready to avoid unnecessary index rescans
- The function assumes the IndexScanState has been properly initialized with relation descriptors and scan keys
- Worker processes will use index_beginscan_parallel with the same shared descriptor to join the parallel scan
- The shared snapshot ensures all workers see a consistent view of the data
- Located in src/backend/executor/nodeIndexscan.c:1661-1696