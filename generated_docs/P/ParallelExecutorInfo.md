# ParallelExecutorInfo

## Location
[src/include/executor/execParallel.h:24-38](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/execParallel.h#L24-L38)

## Overview
ParallelExecutorInfo is a central coordination structure that manages the execution of parallel queries in PostgreSQL, coordinating communication between the leader process and worker processes through shared memory and tuple queues.

## Definition
```c
typedef struct ParallelExecutorInfo
{
    PlanState  *planstate;          /* plan subtree we're running in parallel */
    ParallelContext *pcxt;          /* parallel context we're using */
    BufferUsage *buffer_usage;      /* points to bufusage area in DSM */
    WalUsage   *wal_usage;          /* walusage area in DSM */
    SharedExecutorInstrumentation *instrumentation; /* optional */
    struct SharedJitInstrumentation *jit_instrumentation;    /* optional */
    dsa_area   *area;               /* points to DSA area in DSM */
    dsa_pointer param_exec;         /* serialized PARAM_EXEC parameters */
    bool        finished;           /* set true by ExecParallelFinish */
    /* These two arrays have pcxt->nworkers_launched entries: */
    shm_mq_handle **tqueue;         /* tuple queues for worker output */
    struct TupleQueueReader **reader;   /* tuple reader/writer support */
} ParallelExecutorInfo;
```

## Detailed Description
ParallelExecutorInfo serves as the master control structure for PostgreSQL's parallel query execution system. It is created by the leader process during parallel plan initialization and contains all necessary components to manage parallel worker processes and collect their results.

The structure coordinates several key aspects of parallel execution:
- **Worker Communication**: Manages shared memory message queues (shm_mq_handle) for receiving tuple results from worker processes
- **Resource Monitoring**: Tracks buffer usage and WAL usage statistics across all workers
- **Instrumentation**: Optionally collects performance metrics and JIT compilation statistics from workers  
- **Parameter Sharing**: Serializes and shares PARAM_EXEC parameters with worker processes through DSA (Dynamic Shared Area)
- **Lifecycle Management**: Tracks the completion status of the parallel execution

The structure is allocated in the leader process and remains active throughout the parallel query execution, being cleaned up only when the parallel context is destroyed.

## Parameters / Member Variables
- `planstate`: Pointer to the PlanState node representing the plan subtree being executed in parallel
- `pcxt`: ParallelContext that manages the overall parallel execution environment and worker processes
- `buffer_usage`: Array pointing to BufferUsage statistics collected from each worker process in shared memory
- `wal_usage`: Array pointing to WalUsage statistics collected from each worker process in shared memory
- `instrumentation`: Optional SharedExecutorInstrumentation for collecting detailed execution metrics from workers
- `jit_instrumentation`: Optional SharedJitInstrumentation for collecting JIT compilation statistics from workers
- `area`: DSA (Dynamic Shared Area) used for allocating shared data structures that can be accessed by all processes
- `param_exec`: DSA pointer to serialized PARAM_EXEC parameters that need to be shared with worker processes
- `finished`: Boolean flag set to true by ExecParallelFinish() to indicate parallel execution is complete
- `tqueue`: Array of shared memory message queue handles, one per launched worker, used to receive tuples from workers
- `reader`: Array of TupleQueueReader structures that provide higher-level tuple reading functionality over the raw message queues

## Dependencies
- Functions called/Symbols referenced:
  - ParallelContext
  - BufferUsage  
  - WalUsage
  - SharedExecutorInstrumentation
  - SharedJitInstrumentation
  - dsa_area
  - dsa_pointer
  - shm_mq_handle
  - TupleQueueReader
- Called from (representative examples):
  - ExecInitParallelPlan
  - ExecParallelSetupTupleQueues
  - ExecParallelCreateReaders
  - ExecParallelReinitialize
  - ExecParallelFinish
  - ExecParallelCleanup
  - GatherState
  - GatherMergeState

## Notes and Other Information
- The structure is allocated using palloc0() in the leader process memory context during ExecInitParallelPlan()
- The tqueue and reader arrays are dynamically sized based on pcxt->nworkers_launched
- Worker processes do not directly access this structure; they communicate through the shared memory segments it references
- The structure supports both regular parallel execution (Gather nodes) and sorted parallel execution (GatherMerge nodes)
- Instrumentation and JIT instrumentation are optional features that can be enabled/disabled based on execution parameters
- The DSA area allows for dynamic allocation of shared memory structures that persist across the parallel execution lifecycle
- Resource usage tracking (buffer_usage, wal_usage) provides visibility into worker process resource consumption for monitoring and optimization