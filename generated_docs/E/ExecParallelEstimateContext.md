# ExecParallelEstimateContext

## Location
src/backend/executor/execParallel.c: 111 - 115

## Overview
ExecParallelEstimateContext is a context structure used during the estimation phase of parallel query execution to collect information about shared memory requirements and plan node counts.

## Definition
```c
typedef struct ExecParallelEstimateContext
{
    ParallelContext *pcxt;
    int             nnodes;
} ExecParallelEstimateContext;
```

## Detailed Description
This structure serves as a context object passed during the parallel execution estimation phase (ExecParallelEstimate). It provides a way for the estimation process to accumulate information about the resources needed for parallel execution, specifically tracking the parallel context and counting the number of plan nodes that will participate in parallel execution. This information is crucial for determining the appropriate shared memory allocation and setup requirements before actually initializing the parallel execution environment.

## Parameters / Member Variables
- `pcxt`: Pointer to the ParallelContext structure that manages the overall parallel execution environment and shared memory coordination
- `nnodes`: Integer counter tracking the number of plan nodes that will be involved in the parallel execution

## Dependencies
- Functions called/Symbols referenced:
  - ParallelContext (core parallel execution context structure)
- Called from (representative examples):
  - ExecParallelEstimate (primary function that uses this context for estimation)
  - ExecInitParallelPlan (during parallel plan initialization phase)

## Notes and Other Information
- This structure is used during the planning and setup phase, before actual parallel execution begins
- The nnodes count helps determine memory requirements for structures like SharedExecutorInstrumentation
- Part of PostgreSQL's parallel query execution infrastructure in execParallel.c
- The context pattern allows for clean passing of state through the recursive plan tree estimation process
- Typically short-lived, existing only during the estimation and initialization phases of parallel execution