# _brin_end_parallel

## Location
src/backend/access/brin/brin.c: 2538 - 2568

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
  - WaitForParallelWorkersToFinish (wait for worker completion)
  - InstrAccumParallelQuery (accumulate worker statistics)
  - IsMVCCSnapshot (check snapshot type)
  - UnregisterSnapshot (cleanup MVCC snapshot)
  - DestroyParallelContext (cleanup parallel context)
  - ExitParallelMode (exit parallel execution mode)
- Called from (representative examples):
  - brinbuild (main BRIN index build function)
  - _brin_begin_parallel (when parallel setup fails)

## Notes and Other Information
- Must be called after parallel workers have finished their work
- Accumulates performance instrumentation data from all worker processes
- Handles both regular and concurrent index build cleanup scenarios
- The function ensures proper cleanup even if parallel setup partially failed
- Part of PostgreSQL's parallel index building infrastructure
- The state parameter is part of the function signature but not currently used