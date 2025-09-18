# SharedExecutorInstrumentation

## Location
src/backend/executor/execParallel.c: 97 - 105

## Overview
SharedExecutorInstrumentation is a DSM (Dynamic Shared Memory) structure used for accumulating and sharing per-PlanState instrumentation data across parallel worker processes during parallel query execution.

## Definition
```c
struct SharedExecutorInstrumentation
{
    int         instrument_options;
    int         instrument_offset;
    int         num_workers;
    int         num_plan_nodes;
    int         plan_node_id[FLEXIBLE_ARRAY_MEMBER];
    /* array of num_plan_nodes * num_workers Instrumentation objects follows */
};
```

## Detailed Description
This structure serves as the central repository for collecting execution instrumentation data from multiple parallel workers in PostgreSQL's parallel execution framework. It provides a shared memory layout that allows both the leader and worker processes to access and update execution statistics for various plan nodes. The structure uses a flexible array member design to accommodate variable numbers of plan nodes and workers, with the actual Instrumentation objects stored contiguously after the plan_node_id array.

## Parameters / Member Variables
- `instrument_options`: Configuration flags that control what instrumentation data to collect (same meaning as in instrument.c)
- `instrument_offset`: Byte offset from the start of this structure to the first Instrumentation object, which depends on the length of the plan_node_id array
- `num_workers`: The total number of parallel worker processes participating in the execution
- `num_plan_nodes`: The number of plan nodes for which instrumentation is being collected
- `plan_node_id`: Flexible array containing the identifiers of plan nodes being instrumented, with length equal to num_plan_nodes
- (Following the structure): An array of `num_plan_nodes * num_workers` Instrumentation objects containing the actual performance metrics

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER (macro for variable-length array declaration)
- Called from (representative examples):
  - GetInstrumentationArray (for accessing instrumentation data)
  - ExecParallelInitializeDSMContext (for initialization during DSM context setup)
  - ExecInitParallelPlan (during parallel plan initialization)
  - ExecParallelRetrieveInstrumentation (for collecting data from workers)
  - ExecParallelReportInstrumentation (for reporting collected data)
  - ParallelQueryMain (in worker processes)
  - ParallelExecutorInfo (as part of larger parallel execution structures)

## Notes and Other Information
- This structure is designed for shared memory usage and must be carefully managed across process boundaries
- The variable-size design allows efficient memory usage regardless of the number of plan nodes and workers
- The instrumentation data layout follows a matrix pattern: [worker][plan_node] for easy indexing
- Used extensively in PostgreSQL's EXPLAIN ANALYZE functionality for parallel queries
- The structure is part of the parallel execution infrastructure that enables performance monitoring across distributed query execution