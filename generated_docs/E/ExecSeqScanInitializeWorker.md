# ExecSeqScanInitializeWorker

## Location
[src/backend/executor/nodeSeqscan.c:294-302](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSeqscan.c#L294-L302)

## Overview
ExecSeqScanInitializeWorker initializes a worker process's sequential scan state by copying relevant information from the shared memory table-of-contents into the worker's plan state.

## Definition
```c
void ExecSeqScanInitializeWorker(SeqScanState *node, ParallelWorkerContext *pwcxt)
```

## Detailed Description
This function is called by parallel worker processes to set up their local sequential scan state. It retrieves the parallel scan descriptor from the shared memory table-of-contents using the plan node ID as a key, then initializes the worker's current scan descriptor by beginning a parallel scan using the shared parallel scan descriptor. This allows the worker process to participate in the coordinated parallel sequential scan.

## Parameters / Member Variables
- `node`: A pointer to the SeqScanState structure containing the sequential scan state information for the worker
- `pwcxt`: A pointer to the ParallelWorkerContext structure containing the worker's parallel execution context and shared memory access

## Dependencies
- Functions called/Symbols referenced:
  - [shm_toc_lookup](../s/shm_toc_lookup.md)
  - [table_beginscan_parallel](../t/table_beginscan_parallel.md)
- Types referenced:
  - [SeqScanState](../S/SeqScanState.md)
  - [ParallelWorkerContext](../P/ParallelWorkerContext.md)
  - [ParallelTableScanDesc](../P/ParallelTableScanDesc.md)
- Called from (representative examples):
  - [ExecParallelInitializeWorker](ExecParallelInitializeWorker.md) (in execParallel.c)

## Notes and Other Information
- This function is part of PostgreSQL's parallel query execution infrastructure
- It runs in worker processes during parallel query initialization
- The function assumes that the parallel scan descriptor was previously set up by the leader process using ExecSeqScanInitializeDSM
- The shared memory lookup uses the plan node ID as a key to find the correct parallel scan descriptor
- Each worker gets its own scan descriptor but shares the underlying parallel scan state
- Located in src/backend/executor/nodeSeqscan.c at lines 294-302