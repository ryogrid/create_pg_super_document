# ExecBitmapHeapInitializeDSM

## Location
src/backend/executor/nodeBitmapHeapscan.c: 831 - 864

## Overview
This function initializes a shared memory descriptor for parallel bitmap heap scan operations in PostgreSQL's dynamic shared memory (DSM) framework.

## Definition
```c
void ExecBitmapHeapInitializeDSM(BitmapHeapScanState *node, ParallelContext *pcxt)
```

## Detailed Description
ExecBitmapHeapInitializeDSM sets up the shared state structure needed for coordinating parallel bitmap heap scans across multiple worker processes. The function allocates and initializes a ParallelBitmapHeapState structure in shared memory, which contains synchronization primitives and shared state variables used by all participating processes.

The function performs several key initialization tasks:
1. Allocates shared memory for the ParallelBitmapHeapState structure
2. Initializes shared state fields to default values (tbmiterator, prefetch_iterator both set to 0)
3. Sets up synchronization primitives (spinlock for atomic operations, condition variable for blocking/wakeup)
4. Sets initial state to BM_INITIAL for leader election
5. Registers the shared state in the shared memory TOC for worker process access

The function includes a safety check - if no DSA (Dynamic Shared Area) is available, it returns early since parallel execution is not possible.

## Parameters / Member Variables
- `node`: Pointer to BitmapHeapScanState containing the execution state for this bitmap heap scan node
- `pcxt`: Pointer to ParallelContext structure that manages the parallel execution environment and shared memory allocation

## Dependencies
- Functions called/Symbols referenced:
  - shm_toc_allocate
  - SpinLockInit
  - ConditionVariableInit
  - shm_toc_insert
  - BitmapHeapScanState (structure)
  - ParallelContext (structure)
  - ParallelBitmapHeapState (structure)
  - dsa_area (structure)
  - BM_INITIAL (enum value)
- Called from (representative examples):
  - ExecParallelInitializeDSM

## Notes and Other Information
This function runs only in the leader process during parallel query setup. Worker processes will later access this shared state via ExecBitmapHeapInitializeWorker. The shared state enables coordination for bitmap iterator sharing, prefetch coordination, and synchronization during parallel bitmap heap scanning. The plan_node_id is used as the key for shared memory TOC lookup, ensuring each node's shared state can be uniquely identified.