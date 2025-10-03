# ExecIndexScanInitializeDSM

## Location
[src/backend/executor/nodeIndexscan.c:1661-1696](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeIndexscan.c#L1661-L1696)

## Overview
ExecIndexScanInitializeDSM sets up a parallel index scan descriptor in shared memory, initializing the necessary data structures for coordinated parallel index scanning across multiple worker processes.

## Definition

```c
void
ExecIndexScanInitializeDSM(IndexScanState *node,
						   ParallelContext *pcxt)
```
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
- `*node`: Pointer to IndexScanState containing the index scan execution state, scan keys, and previously calculated shared memory size
- `*pcxt`: Pointer to ParallelContext containing the shared memory table of contents and coordination structures
## Dependencies
- Functions called/Symbols referenced:
  - [shm_toc_allocate](../s/shm_toc_allocate.md)
  - [index_parallelscan_initialize](../i/index_parallelscan_initialize.md)
  - [shm_toc_insert](../s/shm_toc_insert.md)
  - [index_beginscan_parallel](../i/index_beginscan_parallel.md)
  - [index_rescan](../i/index_rescan.md)
- Called from (representative examples):
  - [ExecParallelInitializeDSM](ExecParallelInitializeDSM.md) (in execParallel.c:470)

## Notes and Other Information
- This function works in conjunction with ExecIndexScanEstimate which calculates the required shared memory size
- The parallel index scan descriptor is identified in shared memory by the plan node ID
- Runtime key handling is deferred until all keys are ready to avoid unnecessary index rescans
- The function assumes the IndexScanState has been properly initialized with relation descriptors and scan keys
- Worker processes will use index_beginscan_parallel with the same shared descriptor to join the parallel scan
- The shared snapshot ensures all workers see a consistent view of the data
- Located in src/backend/executor/nodeIndexscan.c:1661-1696

## Simplified Source

```c
void ExecIndexScanInitializeDSM(IndexScanState *node, ParallelContext *pcxt) {
    EState *estate = node->ss.ps.state;

    // Allocate shared memory for parallel index scan descriptor
    ParallelIndexScanDesc piscan = shm_toc_allocate(pcxt->toc, node->iss_PscanLen);

    // Initialize parallel scan with relation and snapshot info
    index_parallelscan_initialize(node->ss.ss_currentRelation,
                                  node->iss_RelationDesc,
                                  estate->es_snapshot,
                                  piscan);

    // Register parallel scan descriptor in shared memory TOC
    shm_toc_insert(pcxt->toc, node->ss.ps.plan->plan_node_id, piscan);

    // Create index scan descriptor for parallel execution
    node->iss_ScanDesc = index_beginscan_parallel(node->ss.ss_currentRelation,
                                                  node->iss_RelationDesc,
                                                  node->iss_NumScanKeys,
                                                  node->iss_NumOrderByKeys,
                                                  piscan);

    // Start scan if runtime keys are ready
    if (node->iss_NumRuntimeKeys == 0 || node->iss_RuntimeKeysReady) {
        index_rescan(node->iss_ScanDesc,
                     node->iss_ScanKeys, node->iss_NumScanKeys,
                     node->iss_OrderByKeys, node->iss_NumOrderByKeys);
    }
}
```