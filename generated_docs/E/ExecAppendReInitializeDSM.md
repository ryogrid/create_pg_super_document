# ExecAppendReInitializeDSM

## Location
src/backend/executor/nodeAppend.c: 524 - 539

## Overview
Resets the shared memory state of a parallel Append node to prepare for a fresh scan, clearing execution progress indicators.

## Definition
```c
void ExecAppendReInitializeDSM(AppendState *node, ParallelContext *pcxt)
```

## Detailed Description
This function reinitializes the shared state for parallel Append execution when beginning a fresh scan. It resets the parallel execution state by setting the next plan index back to 0 and clearing all the finished flags for each subplan. This ensures that when a rescan occurs in parallel execution, all worker processes start from a clean state and can properly coordinate to execute all subplans again.

The function is essential for supporting rescan operations in parallel query execution, where the same Append node may need to be executed multiple times within a single query (e.g., in nested loops).

## Parameters
- `node`: Pointer to the AppendState structure containing the parallel state information
- `pcxt`: Pointer to the ParallelContext structure (used for consistency with other DSM functions, but not directly used in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [ParallelAppendState](../P/ParallelAppendState.md) (struct type)
  - memset
- Called from (representative examples):
  - [ExecParallelReInitializeDSM](ExecParallelReInitializeDSM.md)

## Notes and Other Information
- Resets `pa_next_plan` to 0, indicating that subplan selection should start from the beginning
- Clears the `pa_finished` array using memset, marking all subplans as not finished
- The size of the `pa_finished` array is determined by `node->as_nplans` (number of subplans)
- This function must be called before restarting parallel execution to ensure proper coordination between workers
- Unlike the initial DSM setup, this function only modifies existing shared memory state rather than allocating new structures