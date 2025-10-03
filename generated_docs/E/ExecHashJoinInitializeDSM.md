# ExecHashJoinInitializeDSM

## Location
[src/backend/executor/nodeHashjoin.c:1551-1608](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHashjoin.c#L1551-L1608)

## Overview
Initializes the dynamic shared memory (DSM) structures required for parallel hash join execution by setting up shared state and synchronization primitives.

## Definition
void ExecHashJoinInitializeDSM(HashJoinState *state, ParallelContext *pcxt)

## Detailed Description
ExecHashJoinInitializeDSM is responsible for setting up the shared memory infrastructure needed for parallel hash join operations. This function allocates and initializes the ParallelHashJoinState structure in shared memory, which coordinates access to shared hash tables across multiple parallel worker processes.

The function performs several critical initialization tasks:
1. Verifies that a real DSM segment exists (returns early if not available)
2. Sets the execution function to ExecParallelHashJoin for parallel execution
3. Allocates and inserts ParallelHashJoinState into the shared memory table of contents
4. Initializes all shared state fields with default values
5. Sets up synchronization primitives (locks and barriers) for coordination
6. Initializes shared file set for temporary file management
7. Links the shared state to the inner hash node

The shared state includes batch information, space management, synchronization barriers for different phases of execution, and coordination mechanisms for work distribution among parallel workers.

## Parameters / Member Variables
- `state`: Pointer to the HashJoinState structure representing the hash join execution state
- `pcxt`: Pointer to the ParallelContext structure containing parallel execution context and DSM segment

## Dependencies
- Functions called/Symbols referenced:
  - [ExecSetExecProcNode](ExecSetExecProcNode.md)
  - ExecParallelHashJoin
  - [shm_toc_allocate](../s/shm_toc_allocate.md)
  - [shm_toc_insert](../s/shm_toc_insert.md)
  - [pg_atomic_init_u32](../p/pg_atomic_init_u32.md)
  - [LWLockInitialize](../L/LWLockInitialize.md)
  - [BarrierInit](../B/BarrierInit.md)
  - [SharedFileSetInit](../S/SharedFileSetInit.md)
  - innerPlanState
  - [ParallelHashJoinState](../P/ParallelHashJoinState.md) (struct type)
  - [HashState](../H/HashState.md) (struct type)
  - InvalidDsaPointer (constant)
  - PHJ_GROWTH_OK (constant)
  - LWTRANCHE_PARALLEL_HASH_JOIN (constant)
- Called from (representative examples):
  - [ExecParallelInitializeDSM](ExecParallelInitializeDSM.md)

## Notes and Other Information
- Returns early if no real DSM segment is available, effectively disabling shared hash table mode
- Uses the plan node ID as the table-of-contents key for locating shared state
- Initializes multiple barriers for coordinating different phases: build, grow_batches, and grow_buckets
- The nparticipants field includes both workers and the leader process (pcxt->nworkers + 1)
- Sets up atomic distributor for work distribution among parallel processes
- Essential for enabling parallel hash join execution in PostgreSQL's parallel query framework
- The shared file set enables coordination of temporary files across parallel processes

## Simplified Source

```c
void ExecHashJoinInitializeDSM(HashJoinState *state, ParallelContext *pcxt) {
    int plan_node_id = state->js.ps.plan->plan_node_id;

    // Disable shared hash table if no real DSM segment
    if (pcxt->seg == NULL)
        return;

    // Switch to parallel hash join execution
    ExecSetExecProcNode(&state->js.ps, ExecParallelHashJoin);

    // Allocate and register shared state in DSM
    ParallelHashJoinState *pstate = shm_toc_allocate(pcxt->toc, sizeof(ParallelHashJoinState));
    shm_toc_insert(pcxt->toc, plan_node_id, pstate);

    // Initialize shared state with default values
    pstate->nbatch = 0;
    pstate->space_allowed = 0;
    pstate->batches = InvalidDsaPointer;
    pstate->old_batches = InvalidDsaPointer;
    pstate->nbuckets = 0;
    pstate->growth = PHJ_GROWTH_OK;
    pstate->chunk_work_queue = InvalidDsaPointer;
    pstate->total_tuples = 0;
    pstate->nparticipants = pcxt->nworkers + 1;  // Workers + leader

    // Initialize synchronization primitives
    pg_atomic_init_u32(&pstate->distributor, 0);
    LWLockInitialize(&pstate->lock, LWTRANCHE_PARALLEL_HASH_JOIN);
    BarrierInit(&pstate->build_barrier, 0);
    BarrierInit(&pstate->grow_batches_barrier, 0);
    BarrierInit(&pstate->grow_buckets_barrier, 0);

    // Set up shared temporary file coordination
    SharedFileSetInit(&pstate->fileset, pcxt->seg);

    // Link shared state to inner hash node
    HashState *hashNode = (HashState *) innerPlanState(state);
    hashNode->parallel_state = pstate;
}
```