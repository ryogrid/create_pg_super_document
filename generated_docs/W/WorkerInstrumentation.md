# WorkerInstrumentation

## Location
[src/include/executor/instrument.h:95-99](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/instrument.h#L95-L99)

## Overview
WorkerInstrumentation is a container struct that holds instrumentation data from multiple parallel worker processes, enabling aggregation and reporting of statistics from parallel query execution.

## Definition
```c
typedef struct WorkerInstrumentation
{
    int             num_workers;    /* # of structures that follow */
    Instrumentation instrument[FLEXIBLE_ARRAY_MEMBER];
} WorkerInstrumentation;
```

## Detailed Description
WorkerInstrumentation serves as a collection structure for gathering instrumentation data from parallel worker processes during parallel query execution. It contains a variable-length array of Instrumentation structures, one for each worker process that participated in the parallel operation.

The struct uses a flexible array member to accommodate varying numbers of worker processes, allowing the system to dynamically allocate the appropriate amount of memory based on the actual number of workers involved in a parallel operation. This design is essential for PostgreSQL's parallel query execution infrastructure, enabling the collection and aggregation of performance metrics from distributed execution.

## Parameters / Member Variables
- `num_workers`: The number of worker processes whose instrumentation data is stored in this structure
- `instrument`: Flexible array of Instrumentation structures, one for each worker process

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER (PostgreSQL's flexible array macro)
  - Instrumentation (individual worker instrumentation data)
- Called from (representative examples):
  - ExplainNode (for EXPLAIN output of parallel plans)
  - ExecParallelRetrieveInstrumentation (collecting worker stats)
  - PlanState (execution node state management)

## Notes and Other Information
- The struct is defined in src/include/executor/instrument.h:95-99
- Essential component of PostgreSQL's parallel query execution system
- Used to aggregate performance statistics from multiple worker processes
- The flexible array member allows efficient memory allocation for varying worker counts
- Critical for EXPLAIN ANALYZE output of parallel operations
- Enables detailed analysis of parallel execution performance and load balancing
- The num_workers field indicates how many valid Instrumentation entries follow
- Memory allocation must account for the variable-length nature of this structure