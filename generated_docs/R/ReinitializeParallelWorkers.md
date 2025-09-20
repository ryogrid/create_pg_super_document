# ReinitializeParallelWorkers

## Location
[src/backend/access/transam/parallel.c:554-568](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/parallel.c#L554-L568)

## Overview
Adjusts the number of parallel workers to be launched for an existing parallel context, allowing reuse of the same DSM segment with a different worker count.

## Definition

```c
void
ReinitializeParallelWorkers(ParallelContext *pcxt, int nworkers_to_launch)
```
## Detailed Description
ReinitializeParallelWorkers provides a mechanism to change the number of workers that will be launched when reusing a parallel context. This function is essential for scenarios where the same parallel context and DSM segment need to be reused across multiple operations that may require different numbers of workers, such as parallel vacuum operations processing indexes of varying complexity or parallel query phases with different parallelization requirements.

The function ensures that the requested number of workers does not exceed the maximum number configured when the parallel context was initially created. It gracefully handles situations where InitializeParallelDSM may have reduced the worker count due to system constraints by silently trimming excessive requests rather than failing.

## Parameters / Member Variables
- : The parallel context whose worker count will be adjusted
- : The desired number of workers to launch (will be capped at the context's maximum)

## Dependencies
- Functions called/Symbols referenced:
  - [ParallelContext](../P/ParallelContext.md) (structure being modified)
  - Min (macro to find minimum of two values)

- Called from (representative examples):
  - [parallel_vacuum_process_all_indexes](../p/parallel_vacuum_process_all_indexes.md) (adjusts worker count for different vacuum phases)

## Notes and Other Information
- Does not create or destroy workers, only adjusts the launch count for subsequent LaunchParallelWorkers calls
- Silently caps the request to the maximum workers available in the context (pcxt->nworkers)
- Accounts for potential worker count reductions made by InitializeParallelDSM due to system limitations
- Provides flexibility for multi-phase parallel operations with varying parallelization needs
- Much more efficient than recreating the entire parallel context for different worker counts
- The actual worker launch still requires calling LaunchParallelWorkers after this adjustment