# ExecHashJoinReInitializeDSM

## Location
[src/backend/executor/nodeHashjoin.c:1609-1646](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHashjoin.c#L1609-L1646)

## Overview
Resets shared memory state for parallel hash join operations before beginning a fresh scan, cleaning up previous execution artifacts and reinitializing synchronization primitives.

## Definition
void ExecHashJoinReInitializeDSM(HashJoinState *state, ParallelContext *pcxt)

## Detailed Description
ExecHashJoinReInitializeDSM is responsible for resetting the shared memory state when a parallel hash join needs to be rescanned. This function cleans up any remaining shared memory structures from the previous execution and reinitializes the necessary synchronization primitives to prepare for a fresh scan.

The function performs several cleanup and reinitialization tasks:
1. Verifies that a DSM segment exists (returns early if not available)
2. Looks up the existing ParallelHashJoinState in shared memory
3. Detaches from any existing hash table structures and batches
4. Deletes all shared batch files from the previous execution
5. Reinitializes the build_barrier to allow the parallel hash join to start over

The function includes detailed comments about potential optimizations for single-batch cases, but currently takes a conservative approach of fully cleaning up and restarting rather than attempting to reuse existing structures.

## Parameters / Member Variables
- `state`: Pointer to the HashJoinState structure representing the hash join execution state
- `pcxt`: Pointer to the ParallelContext structure containing parallel execution context and DSM segment

## Dependencies
- Functions called/Symbols referenced:
  - [shm_toc_lookup](../s/shm_toc_lookup.md)
  - [ExecHashTableDetachBatch](ExecHashTableDetachBatch.md)
  - [ExecHashTableDetach](ExecHashTableDetach.md)
  - [SharedFileSetDeleteAll](../S/SharedFileSetDeleteAll.md)
  - [BarrierInit](../B/BarrierInit.md)
  - [ParallelHashJoinState](../P/ParallelHashJoinState.md) (struct type)
- Called from (representative examples):
  - [ExecParallelReInitializeDSM](ExecParallelReInitializeDSM.md)

## Notes and Other Information
- Returns early if no DSM segment is available, consistent with other parallel hash join functions
- Uses the plan node ID to locate the shared state in the table of contents
- Currently does not attempt to reuse shared hash tables for single-batch cases, though this is noted as a potential optimization
- The barrier reinitialization sets the state back to PHJ_BUILD_ELECT to restart the parallel build process
- Essential for supporting rescans in parallel hash join operations
- Ensures clean state between multiple executions of the same parallel hash join
- Part of PostgreSQL's parallel query rescan infrastructure

## Simplified Source
```c
void ExecHashJoinReInitializeDSM(HashJoinState *state, ParallelContext *pcxt) {
    int plan_node_id = state->js.ps.plan->plan_node_id;
    ParallelHashJoinState *pstate;

    // Early exit if no DSM segment exists
    if (pcxt->seg == NULL)
        return;

    // Look up the shared state
    pstate = shm_toc_lookup(pcxt->toc, plan_node_id, false);

    // Detach from existing hash table and batches
    if (state->hj_HashTable != NULL) {
        ExecHashTableDetachBatch(state->hj_HashTable);
        ExecHashTableDetach(state->hj_HashTable);
    }

    // Clean up shared batch files
    SharedFileSetDeleteAll(&pstate->fileset);

    // Reset synchronization barrier for fresh start
    BarrierInit(&pstate->build_barrier, 0);
}
```