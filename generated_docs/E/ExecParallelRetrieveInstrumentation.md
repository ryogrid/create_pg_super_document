# ExecParallelRetrieveInstrumentation

## Location
src/backend/executor/execParallel.c: 1022 - 1090

## Overview
Copies instrumentation statistics from dynamic shared memory to local memory structures, aggregating performance data from all parallel workers for each node in the plan tree.

## Definition
```c
static bool ExecParallelRetrieveInstrumentation(PlanState *planstate, SharedExecutorInstrumentation *instrumentation)
```

## Detailed Description
ExecParallelRetrieveInstrumentation is responsible for collecting and consolidating performance instrumentation data from all parallel workers after query execution completes. The function performs three main tasks:

1. **Locates node instrumentation**: Searches the shared instrumentation array to find data for the current plan node using its plan_node_id
2. **Aggregates worker statistics**: Uses InstrAggNode to accumulate statistics from all parallel workers into the main planstate instrumentation
3. **Preserves per-worker detail**: Allocates and copies individual worker instrumentation data for detailed analysis, stored in the query memory context
4. **Handles node-specific retrieval**: Calls specialized retrieval functions for certain node types (Sort, Hash, Aggregate, etc.) that maintain additional instrumentation data

The function recursively traverses the entire plan tree using planstate_tree_walker to ensure all nodes' instrumentation is properly retrieved and aggregated.

## Parameters / Member Variables
- `planstate`: The PlanState node for which instrumentation data should be retrieved
- `instrumentation`: The SharedExecutorInstrumentation containing performance data from all parallel workers

## Dependencies
- Functions called/Symbols referenced:
  - GetInstrumentationArray
  - [InstrAggNode](../I/InstrAggNode.md)
  - nodeTag
  - [mul_size](../m/mul_size.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [palloc](../p/palloc.md)
  - memcpy
  - [ExecSortRetrieveInstrumentation](ExecSortRetrieveInstrumentation.md)
  - [ExecIncrementalSortRetrieveInstrumentation](ExecIncrementalSortRetrieveInstrumentation.md)
  - ExecHashRetrieveInstrumentation
  - [ExecAggRetrieveInstrumentation](ExecAggRetrieveInstrumentation.md)
  - [ExecMemoizeRetrieveInstrumentation](ExecMemoizeRetrieveInstrumentation.md)
  - planstate_tree_walker
- Called from (representative examples):
  - [ExecParallelCleanup](ExecParallelCleanup.md)
  - [ExecParallelRetrieveInstrumentation](ExecParallelRetrieveInstrumentation.md) (recursive calls)

## Notes and Other Information
- This is a static function internal to execParallel.c
- Allocates worker_instrument in the per-query memory context to match the lifetime of regular instrumentation
- Only certain node types (Sort, IncrementalSort, Hash, Agg, Memoize) have specialized instrumentation retrieval
- The function will error if a plan node's instrumentation cannot be found in the shared memory structure
- Critical for providing detailed performance analysis of parallel query execution
- Preserves both aggregated statistics and individual worker details for comprehensive performance profiling