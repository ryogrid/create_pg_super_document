# _bt_end_parallel

## Location
[src/backend/access/nbtree/nbtsort.c:1607-1632](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsort.c#L1607-L1632)

## Overview
Cleanly terminates parallel B-tree index construction by shutting down worker processes, accumulating performance statistics, and cleaning up parallel context resources.

## Definition

```c
static void
_bt_end_parallel(BTLeader *btleader)
```
## Detailed Description
This function performs the orderly shutdown of a parallel B-tree index build operation. It ensures all worker processes complete their work, collects and accumulates performance monitoring data (WAL and buffer usage statistics) from all workers, and properly releases all parallel execution resources.

The function is essential for maintaining system consistency and preventing resource leaks during parallel index builds. It handles both successful completion scenarios and error/fallback cases where parallel processing needs to be terminated early.

Key responsibilities include:
- Waiting for all launched worker processes to complete their tasks
- Collecting performance instrumentation data from each worker
- Properly releasing MVCC snapshots used during concurrent builds
- Destroying the parallel context and associated shared memory
- Exiting PostgreSQL's parallel execution mode

## Parameters / Member Variables
- : Pointer to BTLeader structure containing parallel context, worker information, performance monitoring arrays, and snapshot data

## Dependencies
- Functions called/Symbols referenced:
  - [WaitForParallelWorkersToFinish](../W/WaitForParallelWorkersToFinish.md): Wait for all worker processes to complete
  - [InstrAccumParallelQuery](../I/InstrAccumParallelQuery.md): Accumulate performance statistics from worker processes
  - IsMVCCSnapshot: Check if snapshot is MVCC type (used in concurrent builds)
  - [UnregisterSnapshot](../U/UnregisterSnapshot.md): Release MVCC snapshot reference
  - [DestroyParallelContext](../D/DestroyParallelContext.md): Clean up parallel context and shared memory
  - [ExitParallelMode](../E/ExitParallelMode.md): Exit PostgreSQL's parallel execution mode
- Called from (representative examples):
  - [btbuild](btbuild.md): Main B-tree build function when completing parallel build
  - [_bt_begin_parallel](_bt_begin_parallel.md): Fallback cleanup when parallel setup fails

## Notes and Other Information
- Must be called for every successful _bt_begin_parallel() call to prevent resource leaks
- Performance data accumulation requires workers to finish completely to ensure data completeness
- Handles both regular and concurrent index build cleanup through snapshot type checking
- Critical for maintaining proper parallel execution state in PostgreSQL
- Function is idempotent and safe to call in error recovery scenarios