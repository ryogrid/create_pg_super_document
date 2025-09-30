# ExecParallelFinish

## Location
[src/backend/executor/execParallel.c:1131-1183](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execParallel.c#L1131-L1183)

## Overview
Initiates the graceful termination of parallel query execution by waiting for workers to complete and accumulating their resource usage statistics.

## Definition
```c
void ExecParallelFinish(ParallelExecutorInfo *pei)
```

## Detailed Description
ExecParallelFinish orchestrates the orderly shutdown of parallel query execution. The function performs several critical cleanup operations in a specific sequence:

1. **Prevents duplicate execution**: Checks the finished flag to ensure the function can be called multiple times safely without side effects

2. **Signals workers to stop**: Detaches from tuple message queues (shm_mq_detach) to notify still-active workers that no further results are needed, encouraging them to terminate gracefully

3. **Cleans up local resources**: Destroys tuple queue readers using DestroyTupleQueueReader and frees associated memory, removing local infrastructure before waiting for workers

4. **Waits for worker completion**: Calls WaitForParallelWorkersToFinish to ensure all workers have terminated before proceeding with final cleanup

5. **Accumulates resource usage**: Collects buffer usage and WAL (Write-Ahead Log) usage statistics from all workers using InstrAccumParallelQuery, providing complete resource accounting for the parallel operation

The function ensures proper resource cleanup and prevents resource leaks during parallel query termination.

## Parameters / Member Variables
- `pei`: The ParallelExecutorInfo structure containing parallel execution state, worker information, tuple queues, and resource usage tracking data

## Dependencies
- Functions called/Symbols referenced:
  - [shm_mq_detach](../s/shm_mq_detach.md)
  - [pfree](../p/pfree.md)
  - [DestroyTupleQueueReader](../D/DestroyTupleQueueReader.md)
  - [WaitForParallelWorkersToFinish](../W/WaitForParallelWorkersToFinish.md)
  - [InstrAccumParallelQuery](../I/InstrAccumParallelQuery.md)
- Called from (representative examples):
  - [ExecShutdownGatherWorkers](ExecShutdownGatherWorkers.md)
  - [ExecShutdownGatherMergeWorkers](ExecShutdownGatherMergeWorkers.md)

## Notes and Other Information
- This is a public function used by parallel-aware execution nodes
- The function is idempotent - calling it multiple times has no adverse effects due to the finished flag check
- Resource cleanup occurs in a specific order: first signal workers, then clean local resources, then wait for workers, finally accumulate statistics
- Critical for proper resource management and preventing deadlocks during parallel query shutdown
- Must be called before ExecParallelCleanup to ensure workers have finished before final memory cleanup
- Accumulation of buffer and WAL usage provides essential statistics for query performance analysis and resource planning

## Simplified Source

```c
void ExecParallelFinish(ParallelExecutorInfo *pei) {
    int nworkers = pei->pcxt->nworkers_launched;
    int i;

    // Prevent duplicate execution
    if (pei->finished) {
        return;
    }

    // Signal workers to stop by detaching from tuple queues
    if (pei->tqueue != NULL) {
        for (i = 0; i < nworkers; i++) {
            shm_mq_detach(pei->tqueue[i]);
        }
        pfree(pei->tqueue);
        pei->tqueue = NULL;
    }

    // Clean up local tuple queue readers
    if (pei->reader != NULL) {
        for (i = 0; i < nworkers; i++) {
            DestroyTupleQueueReader(pei->reader[i]);
        }
        pfree(pei->reader);
        pei->reader = NULL;
    }

    // Wait for all workers to complete
    WaitForParallelWorkersToFinish(pei->pcxt);

    // Accumulate resource usage statistics from all workers
    for (i = 0; i < nworkers; i++) {
        InstrAccumParallelQuery(&pei->buffer_usage[i], &pei->wal_usage[i]);
    }

    // Mark as finished to prevent re-execution
    pei->finished = true;
}
```