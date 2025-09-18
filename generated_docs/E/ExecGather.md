# ExecGather

## Location
src/backend/executor/nodeGather.c: 137 - 243

## Overview
The main execution function for Gather plan nodes that coordinates parallel query execution by collecting tuples from multiple worker processes and optionally executing the plan locally.

## Definition


## Detailed Description
ExecGather implements the core logic for parallel query execution coordination in PostgreSQL. On first execution, it initializes the parallel context and launches worker processes if parallel execution is enabled and workers are available. The function manages a dynamic decision about whether the leader process should participate in scanning based on worker availability and configuration settings.

The execution strategy adapts based on runtime conditions: if no workers are launched or if parallel_leader_participation is enabled and the plan is not single-copy, the leader process executes the plan locally. Otherwise, it purely coordinates workers. The function uses gather_getnext to retrieve the next tuple, which implements the complex logic of reading from multiple sources (workers and/or local execution). If projection is needed, it applies the projection using ExecProject.

## Parameters / Member Variables
- : The plan state containing execution context and configuration, cast to GatherState

## Dependencies
- Functions called/Symbols referenced:
  - castNode (safely casts PlanState to GatherState)
  - CHECK_FOR_INTERRUPTS (allows query cancellation)
  - ExecInitParallelPlan (sets up parallel execution infrastructure)
  - ExecParallelReinitialize (reinitializes parallel context for reuse)
  - LaunchParallelWorkers (starts the actual worker processes)
  - ExecParallelCreateReaders (sets up tuple queue readers)
  - ResetExprContext (cleans up per-tuple memory)
  - gather_getnext (retrieves next tuple from workers/local)
  - TupIsNull (checks for end-of-data condition)
  - ExecProject (applies projection if needed)
- Called from (representative examples):
  - Set as ExecProcNode function pointer in ExecInitGather

## Notes and Other Information
- Defers parallel context initialization to first execution rather than node initialization to avoid unnecessary resource allocation
- Dynamically determines whether leader participation is needed based on actual worker availability
- Maintains an array of active TupleQueueReader pointers for efficient worker communication
- Uses round-robin reading strategy through the nextreader field
- Handles the case where no workers are available by falling back to local execution
- The number of workers launched may be less than requested and is saved for EXPLAIN output
- Memory context is reset per tuple to prevent memory leaks during long-running queries