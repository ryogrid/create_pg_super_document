# ExecShutdownGatherWorkers

## Location
[src/backend/executor/nodeGather.c:393-410](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeGather.c#L393-L410)

## Overview
Stops all parallel workers associated with a Gather node and cleans up related resources.

## Definition


## Detailed Description
ExecShutdownGatherWorkers is a static helper function that handles the shutdown of parallel workers in PostgreSQL's Gather executor node. It performs two main cleanup tasks: first, it calls ExecParallelFinish to properly terminate all parallel worker processes, and second, it deallocates the reader array that was used to communicate with those workers. This function ensures that all parallel execution resources are properly cleaned up when they are no longer needed.

## Parameters / Member Variables
- : A pointer to the GatherState structure containing the parallel execution information and reader array to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - [ExecParallelFinish](ExecParallelFinish.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [gather_readnext](../g/gather_readnext.md)
  - [ExecShutdownGather](ExecShutdownGather.md)  
  - [ExecReScanGather](ExecReScanGather.md)

## Notes and Other Information
This is a static function internal to nodeGather.c and is not exposed outside the module. It serves as a centralized cleanup routine used by multiple functions that need to shut down parallel workers. The function safely handles null pointers for both the parallel execution info (pei) and reader array, making it safe to call multiple times or when no parallel workers were actually started.