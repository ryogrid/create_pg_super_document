# ExecSeqScanInitializeDSM

## Location
[src/backend/executor/nodeSeqscan.c:256-277](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSeqscan.c#L256-L277)

## Overview
ExecSeqScanInitializeDSM sets up a parallel heap scan descriptor in shared memory for parallel sequential scan operations.

## Definition
```c
void ExecSeqScanInitializeDSM(SeqScanState *node, ParallelContext *pcxt)
```

## Detailed Description
This function initializes the Dynamic Shared Memory (DSM) structures required for parallel sequential scans. It allocates shared memory for the parallel scan descriptor, initializes it with the current relation and snapshot information, and inserts it into the shared memory table-of-contents. Finally, it begins a parallel scan using the initialized descriptor, setting up the scan state for parallel execution.

## Parameters / Member Variables
- `node`: A pointer to the SeqScanState structure containing the sequential scan state information
- `pcxt`: A pointer to the ParallelContext structure that manages parallel execution context and shared memory

## Dependencies
- Functions called/Symbols referenced:
  - [shm_toc_allocate](../s/shm_toc_allocate.md)
  - [table_parallelscan_initialize](../t/table_parallelscan_initialize.md)
  - [shm_toc_insert](../s/shm_toc_insert.md)
  - [table_beginscan_parallel](../t/table_beginscan_parallel.md)
- Types referenced:
  - [SeqScanState](../S/SeqScanState.md)
  - [ParallelContext](../P/ParallelContext.md)
  - [EState](EState.md)
  - [ParallelTableScanDesc](../P/ParallelTableScanDesc.md)
- Called from (representative examples):
  - [ExecParallelInitializeDSM](ExecParallelInitializeDSM.md) (in execParallel.c)

## Notes and Other Information
- This function is part of PostgreSQL's parallel query execution infrastructure
- It operates during the initialization phase of parallel query execution
- The function uses the pscan_len value that was previously calculated by ExecSeqScanEstimate
- The parallel scan descriptor is identified in the shared memory TOC using the plan node ID
- The function establishes the current scan descriptor for use in parallel execution
- Located in src/backend/executor/nodeSeqscan.c at lines 256-277

## Simplified Source

```c
void ExecSeqScanInitializeDSM(SeqScanState *node, ParallelContext *pcxt) {
    EState *estate = node->ss.ps.state;

    // Allocate shared memory for parallel table scan descriptor
    ParallelTableScanDesc pscan = shm_toc_allocate(pcxt->toc, node->pscan_len);

    // Initialize parallel scan with relation and snapshot
    table_parallelscan_initialize(node->ss.ss_currentRelation,
                                  pscan,
                                  estate->es_snapshot);

    // Register parallel scan descriptor in shared memory TOC
    shm_toc_insert(pcxt->toc, node->ss.ps.plan->plan_node_id, pscan);

    // Begin parallel scan using the shared descriptor
    node->ss.ss_currentScanDesc = table_beginscan_parallel(node->ss.ss_currentRelation, pscan);
}
```