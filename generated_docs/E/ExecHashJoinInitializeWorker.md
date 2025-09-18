# ExecHashJoinInitializeWorker

## Location
[src/backend/executor/nodeHashjoin.c:1647-1663](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHashjoin.c#L1647-L1663)

## Overview
Initializes a worker process for parallel hash join execution by attaching to shared memory structures and configuring the hash join state for parallel operation.

## Definition
```c
void ExecHashJoinInitializeWorker(HashJoinState *state, ParallelWorkerContext *pwcxt)
```

## Detailed Description
This function is responsible for setting up a worker process to participate in a parallel hash join operation. It performs critical initialization steps to connect the worker to the shared parallel execution environment:

1. **Shared State Attachment**: Looks up and attaches to the ParallelHashJoinState stored in shared memory using the plan node ID as a key
2. **Shared File System Setup**: Attaches to the shared temporary file set that may be used for spilling hash table data to disk during parallel execution
3. **Hash Node Configuration**: Links the hash node (inner plan) to the shared parallel state so it can coordinate with other workers
4. **Execution Function Override**: Sets the execution function to ExecParallelHashJoin, which is the parallel-aware version of the hash join executor

This initialization is essential for coordinating multiple worker processes that collectively build and probe a shared hash table, enabling PostgreSQL's parallel hash join functionality.

## Parameters / Member Variables
- `state`: Pointer to the HashJoinState structure representing the current hash join node's execution state
- `pwcxt`: Pointer to the ParallelWorkerContext containing shared memory and coordination information for the parallel worker

## Dependencies
- Functions called/Symbols referenced:
  - [shm_toc_lookup](../s/shm_toc_lookup.md)
  - SharedFileSetAttach
  - innerPlanState
  - [ExecSetExecProcNode](ExecSetExecProcNode.md)
  - ExecParallelHashJoin
- Types referenced:
  - [HashJoinState](../H/HashJoinState.md)
  - [ParallelWorkerContext](../P/ParallelWorkerContext.md)
  - [HashState](../H/HashState.md)
  - ParallelHashJoinState
- Called from (representative examples):
  - [ExecParallelInitializeWorker](ExecParallelInitializeWorker.md) (in src/backend/executor/execParallel.c:1351)

## Notes and Other Information
- This function is part of PostgreSQL's parallel query execution infrastructure, specifically for hash joins
- The function assumes that the ParallelHashJoinState has already been created and stored in the shared memory table of contents by the leader process
- After calling this function, the worker will use ExecParallelHashJoin instead of the regular ExecHashJoin for tuple processing
- The shared file set attachment enables workers to coordinate when hash table data needs to be spilled to temporary files
- This is a void function that modifies the passed HashJoinState structure in place
- Located in src/backend/executor/nodeHashjoin.c:1647-1663