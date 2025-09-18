# ExecAppendInitializeDSM

## Location
[src/backend/executor/nodeAppend.c:503-523](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAppend.c#L503-L523)

## Overview
Initializes shared memory state for parallel execution of Append nodes, setting up the distributed shared memory (DSM) structures needed for worker processes to coordinate.

## Definition
```c
void ExecAppendInitializeDSM(AppendState *node, ParallelContext *pcxt)
```

## Detailed Description
This function sets up the shared memory infrastructure required for parallel execution of Append nodes. It allocates a `ParallelAppendState` structure in shared memory using the shared memory table of contents (TOC), initializes a lightweight lock for synchronization between parallel workers, and configures the leader process to use the appropriate subplan selection function for parallel execution.

The function is part of PostgreSQL's parallel query execution framework, specifically handling the initialization phase where the leader process prepares shared state that will be accessed by multiple worker processes executing different subplans of the Append node.

## Parameters
- `node`: Pointer to the AppendState structure representing the current Append node execution state
- `pcxt`: Pointer to the ParallelContext structure containing shared memory management information

## Dependencies
- Functions called/Symbols referenced:
  - shm_toc_allocate
  - LWLockInitialize
  - shm_toc_insert
  - [choose_next_subplan_for_leader](../c/choose_next_subplan_for_leader.md)
- Called from (representative examples):
  - [ExecParallelInitializeDSM](ExecParallelInitializeDSM.md)

## Notes and Other Information
- The function allocates shared memory based on the pre-calculated `pstate_len` field in the AppendState
- Uses LWTRANCHE_PARALLEL_APPEND tranche for the lightweight lock to enable proper lock monitoring and debugging
- Sets the leader process to use `choose_next_subplan_for_leader` function for coordinating work distribution among parallel workers
- The allocated shared memory is inserted into the TOC using the plan node ID as the key for later retrieval by worker processes