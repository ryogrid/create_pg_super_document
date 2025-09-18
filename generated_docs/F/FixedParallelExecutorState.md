# FixedParallelExecutorState

## Location
[src/backend/executor/execParallel.c:73-79](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execParallel.c#L73-L79)

## Overview
FixedParallelExecutorState is a structure that contains fixed-size configuration and state information that needs to be passed from the leader process to parallel worker processes during parallel query execution.

## Definition
```c
typedef struct FixedParallelExecutorState
{
    int64       tuples_needed;  /* tuple bound, see ExecSetTupleBound */
    dsa_pointer param_exec;
    int         eflags;
    int         jit_flags;
} FixedParallelExecutorState;
```

## Detailed Description
This structure serves as a communication mechanism between the parallel query leader and worker processes in PostgreSQL's parallel execution framework. It contains essential execution parameters that remain constant throughout the parallel execution and need to be shared with all worker processes. The structure is designed to be of fixed size to facilitate efficient serialization and transmission to worker processes through shared memory.

## Parameters / Member Variables
- `tuples_needed`: A 64-bit integer specifying the maximum number of tuples that should be processed, used for tuple bound optimization (see ExecSetTupleBound)
- `param_exec`: A DSA (Dynamic Shared Area) pointer to parameter execution state that needs to be shared across parallel processes
- `eflags`: Integer flags controlling executor behavior and options for the parallel execution
- `jit_flags`: Integer flags specifically related to Just-In-Time compilation settings for the parallel execution

## Dependencies
- Functions called/Symbols referenced:
  - dsa_pointer (DSA pointer type for shared memory management)
- Called from (representative examples):
  - [ExecInitParallelPlan](../E/ExecInitParallelPlan.md) (multiple references for initialization and setup)
  - [ExecParallelReinitialize](../E/ExecParallelReinitialize.md) (for reinitializing parallel execution state)
  - [ParallelQueryMain](../P/ParallelQueryMain.md) (in the main parallel worker entry point)

## Notes and Other Information
- This structure is part of PostgreSQL's parallel query execution infrastructure located in execParallel.c
- The fixed-size nature of this structure is crucial for efficient inter-process communication in parallel execution
- The structure is typically embedded within larger parallel execution state structures
- All members are designed to be safely shared across process boundaries without requiring complex serialization