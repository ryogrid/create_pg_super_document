# ExecHashJoinEstimate

## Location
[src/backend/executor/nodeHashjoin.c:1544-1550](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHashjoin.c#L1544-L1550)

## Overview
Estimates the shared memory requirements for parallel hash join execution by calculating space needed for parallel state structures.

## Definition
void ExecHashJoinEstimate(HashJoinState *state, ParallelContext *pcxt)

## Detailed Description
ExecHashJoinEstimate is responsible for estimating the shared memory requirements when setting up a parallel hash join operation. This function is called during the parallel query planning phase to determine how much shared memory should be allocated for the parallel hash join state.

The function performs two key estimations:
1. Estimates the space needed for a ParallelHashJoinState structure using shm_toc_estimate_chunk()
2. Estimates the space needed for one shared memory table-of-contents key using shm_toc_estimate_keys()

These estimations are crucial for proper shared memory allocation in PostgreSQL's parallel query execution framework, ensuring that sufficient memory is available for coordinating parallel hash join workers.

## Parameters / Member Variables
- `state`: Pointer to the HashJoinState structure representing the hash join execution state
- `pcxt`: Pointer to the ParallelContext structure containing parallel execution context and memory estimator

## Dependencies
- Functions called/Symbols referenced:
  - shm_toc_estimate_chunk
  - shm_toc_estimate_keys
  - ParallelHashJoinState (struct type)
  - ParallelContext (struct type)
  - HashJoinState (struct type)
- Called from (representative examples):
  - ExecParallelEstimate

## Notes and Other Information
- Part of PostgreSQL's parallel query execution infrastructure
- The single key estimation corresponds to the shared hash join state entry in the shared memory table of contents
- This function only performs estimation; actual shared memory allocation happens in ExecHashJoinInitializeDSM
- Essential for proper resource planning in parallel hash join operations
- Works in conjunction with other parallel execution estimation functions to determine total memory requirements