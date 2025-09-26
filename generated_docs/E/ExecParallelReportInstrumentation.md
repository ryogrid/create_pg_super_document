# ExecParallelReportInstrumentation

## Location
[src/backend/executor/execParallel.c:1268-1308](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execParallel.c#L1268-L1308)

## Overview
Copies instrumentation information from a plan node and its descendants into shared memory so the parallel leader can retrieve execution statistics from parallel workers.

## Definition
```c
static bool ExecParallelReportInstrumentation(PlanState *planstate, SharedExecutorInstrumentation *instrumentation)
```

## Detailed Description
This static function is part of PostgreSQL's parallel query instrumentation system. It traverses the plan tree and collects execution statistics (timing, tuple counts, buffer usage, etc.) from parallel workers, storing them in shared memory where the leader process can aggregate them later. The function performs a linear search to find the appropriate slot for each plan node's statistics and aggregates the worker's instrumentation data into the shared structure.

The function uses a planstate_tree_walker to recursively process all nodes in the plan tree, ensuring that statistics from all executed plan nodes are captured. It handles the mapping between plan node IDs and their positions in the shared instrumentation array, and aggregates worker-specific statistics into per-worker slots.

## Parameters / Member Variables
- `planstate`: Current plan state node being processed for instrumentation reporting
- `instrumentation`: Shared memory structure containing instrumentation data for all workers and plan nodes

## Dependencies
- Functions called/Symbols referenced:
  - [InstrEndLoop](../I/InstrEndLoop.md)
  - GetInstrumentationArray  
  - IsParallelWorker
  - [InstrAggNode](../I/InstrAggNode.md)
  - planstate_tree_walker
  - elog (for error reporting)
- Global variables used:
  - ParallelWorkerNumber
- Types used:
  - [PlanState](../P/PlanState.md)
  - [SharedExecutorInstrumentation](../S/SharedExecutorInstrumentation.md)
  - [Instrumentation](../I/Instrumentation.md)
- Called from:
  - [ParallelQueryMain](../P/ParallelQueryMain.md)
  - [ExecParallelReportInstrumentation](ExecParallelReportInstrumentation.md) (recursive self-call via planstate_tree_walker)

## Notes and Other Information
- This is a static function, only accessible within execParallel.c
- Uses linear search for plan node lookup - comments indicate binary search could be implemented for large plan trees
- The function is designed to handle cases where workers might be relaunched during execution
- Includes assertions to verify the function is running in a parallel worker context
- Statistics are stored per-worker to allow the leader to understand individual worker performance
- Part of the broader parallel query instrumentation framework that enables EXPLAIN ANALYZE to work with parallel queries
- The recursive nature via planstate_tree_walker ensures all nodes in the execution tree are processed