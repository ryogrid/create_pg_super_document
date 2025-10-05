# _brin_end_parallel

## Location
[src/backend/access/brin/brin.c:2538-2568](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin.c#L2538-L2568)

## Overview
Shuts down parallel BRIN index building by terminating worker processes, collecting performance statistics, and cleaning up parallel execution resources.

## Definition
```c
static void _brin_end_parallel(BrinLeader *brinleader, BrinBuildState *state)
```

## Detailed Description
This function performs the cleanup and finalization tasks for parallel BRIN index building:

1. **Worker termination**: Waits for all launched worker processes to complete their work and finish execution
2. **Statistics accumulation**: Collects WAL usage and buffer usage statistics from all worker processes and accumulates them into the main instrumentation counters
3. **Snapshot cleanup**: Unregisters the MVCC snapshot if one was used (in concurrent index builds)
4. **Resource cleanup**: Destroys the parallel context and exits parallel mode

The function ensures that all parallel resources are properly released and that performance statistics from worker processes are properly integrated into the overall build statistics.

## Parameters / Member Variables
- `brinleader`: Pointer to BrinLeader structure containing parallel execution state and shared resources
- `state`: Pointer to BrinBuildState (currently unused in the function but part of the interface)

## Dependencies
- Functions called/Symbols referenced:
  - [WaitForParallelWorkersToFinish](../W/WaitForParallelWorkersToFinish.md) (wait for worker completion)
  - [InstrAccumParallelQuery](../I/InstrAccumParallelQuery.md) (accumulate worker statistics)
  - IsMVCCSnapshot (check snapshot type)
  - [UnregisterSnapshot](../U/UnregisterSnapshot.md) (cleanup MVCC snapshot)
  - [DestroyParallelContext](../D/DestroyParallelContext.md) (cleanup parallel context)
  - [ExitParallelMode](../E/ExitParallelMode.md) (exit parallel execution mode)
- Called from (representative examples):
  - [brinbuild](brinbuild.md) (main BRIN index build function)
  - [_brin_begin_parallel](_brin_begin_parallel.md) (when parallel setup fails)

## Notes and Other Information
- Must be called after parallel workers have finished their work
- Accumulates performance instrumentation data from all worker processes
- Handles both regular and concurrent index build cleanup scenarios
- The function ensures proper cleanup even if parallel setup partially failed
- Part of PostgreSQL's parallel index building infrastructure
- The state parameter is part of the function signature but not currently used

## Simplified Source
```c
static void
_brin_end_parallel(BrinLeader *brinleader, BrinBuildState *state)
{
    int i;

    // Wait for all worker processes to finish
    WaitForParallelWorkersToFinish(brinleader->pcxt);

    // Accumulate WAL and buffer usage statistics from all workers
    for (i = 0; i < brinleader->pcxt->nworkers_launched; i++)
        InstrAccumParallelQuery(&brinleader->bufferusage[i],
                               &brinleader->walusage[i]);

    // Clean up snapshot if using MVCC
    if (IsMVCCSnapshot(brinleader->snapshot))
        UnregisterSnapshot(brinleader->snapshot);

    // Destroy parallel context and exit parallel mode
    DestroyParallelContext(brinleader->pcxt);
    ExitParallelMode();
}
```