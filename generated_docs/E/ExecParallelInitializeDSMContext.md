# ExecParallelInitializeDSMContext

## Location
[src/backend/executor/execParallel.c:118-123](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execParallel.c#L118-L123)

## Overview
ExecParallelInitializeDSMContext is a context structure used during the Dynamic Shared Memory (DSM) initialization phase of parallel query execution to coordinate the setup of shared memory structures and instrumentation.

## Definition
```c
typedef struct ExecParallelInitializeDSMContext
{
    ParallelContext *pcxt;
    SharedExecutorInstrumentation *instrumentation;
    int             nnodes;
} ExecParallelInitializeDSMContext;
```

## Detailed Description
This structure serves as a context object passed during the DSM initialization phase (ExecParallelInitializeDSM) of parallel query execution. It coordinates the setup of shared memory structures that will be used by both the leader and worker processes during parallel execution. The context maintains references to the parallel execution environment, shared instrumentation infrastructure, and tracks the number of plan nodes being initialized. This ensures that all necessary shared memory structures are properly established before parallel workers begin execution.

## Parameters / Member Variables
- `pcxt`: Pointer to the ParallelContext structure that manages the overall parallel execution environment and coordinates shared memory allocation
- `instrumentation`: Pointer to the SharedExecutorInstrumentation structure used for collecting performance metrics across all parallel workers
- `nnodes`: Integer counter tracking the number of plan nodes being initialized in the shared memory context

## Dependencies
- Functions called/Symbols referenced:
  - [ParallelContext](../P/ParallelContext.md) (core parallel execution context structure)
  - [SharedExecutorInstrumentation](../S/SharedExecutorInstrumentation.md) (shared memory structure for performance metrics)
- Called from (representative examples):
  - [ExecParallelInitializeDSM](ExecParallelInitializeDSM.md) (primary function that uses this context for DSM initialization)
  - [ExecInitParallelPlan](ExecInitParallelPlan.md) (during the parallel plan initialization process)

## Notes and Other Information
- This structure is used during the DSM setup phase, after estimation but before worker processes are launched
- The context ensures consistent initialization of shared memory structures across the entire plan tree
- Part of PostgreSQL's parallel query execution infrastructure in execParallel.c
- The nnodes count helps ensure all plan nodes are properly initialized in shared memory
- Critical for establishing the shared memory layout that enables communication between leader and worker processes
- Used in conjunction with ExecParallelEstimateContext during the complete parallel setup process