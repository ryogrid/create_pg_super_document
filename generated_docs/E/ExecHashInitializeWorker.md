# ExecHashInitializeWorker

## Location
src/backend/executor/nodeHash.c: 2785 - 2810

## Overview
ExecHashInitializeWorker locates and connects a parallel worker to its designated shared memory space for hash table instrumentation data collection.

## Definition
```c
void ExecHashInitializeWorker(HashState *node, ParallelWorkerContext *pwcxt)
```

## Detailed Description
This function is called during parallel worker initialization to establish a connection between the worker and its dedicated instrumentation data area in shared memory. Each parallel worker gets its own slot in a shared array where it can record hash table performance metrics.

The function performs the following operations:
1. Checks if instrumentation is enabled for this hash node
2. Looks up the shared instrumentation area using the plan node ID
3. Sets up a direct pointer to this worker's specific instrumentation slot

The worker uses ParallelWorkerNumber (a global variable) to identify which slot in the shared array belongs to it. This pointer will be used later when the worker shuts down or rebuilds the hash table to accumulate its performance statistics.

## Parameters / Member Variables
- `node`: HashState pointer representing the hash node execution state that will be updated with the instrumentation pointer
- `pwcxt`: ParallelWorkerContext pointer containing the parallel worker context with access to the shared memory table of contents

## Dependencies
- Functions called/Symbols referenced:
  - shm_toc_lookup
- Types used:
  - HashState
  - ParallelWorkerContext
  - SharedHashInfo
- Global variables used:
  - ParallelWorkerNumber
- Called from (representative examples):
  - ExecParallelInitializeWorker

## Notes and Other Information
- The function returns early if instrumentation is disabled (node->ps.instrument is NULL)
- Each worker gets a unique slot in the shared instrumentation array based on its ParallelWorkerNumber
- The shared memory lookup uses the plan node ID as the key, which was registered during ExecHashInitializeDSM
- The established pointer (node->hinstrument) will be used by ExecHashAccumInstrumentation to record statistics
- This function is part of PostgreSQL's parallel query execution infrastructure for coordinating instrumentation data collection across workers