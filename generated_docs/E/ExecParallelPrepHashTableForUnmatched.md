# ExecParallelPrepHashTableForUnmatched

## Location
src/backend/executor/nodeHash.c: 2104 - 2168

## Overview
Prepares for scanning unmatched tuples in a parallel hash join by coordinating between worker processes to ensure only one worker performs the unmatched scan for each batch.

## Definition
```c
bool ExecParallelPrepHashTableForUnmatched(HashJoinState *hjstate)
```

## Detailed Description
This function handles the complex coordination required for parallel hash joins when transitioning to scan for unmatched inner tuples (needed for RIGHT JOIN and FULL OUTER JOIN operations). The challenge in parallel execution is ensuring that only one worker process performs the unmatched scan for each batch while others can continue working on different batches.

The function implements a wait-free election mechanism:
1. Verifies that the current batch is in PHJ_BATCH_PROBE phase
2. Uses BarrierArriveAndDetachExceptLast() to elect one worker to continue
3. Non-elected workers detach from the batch and mark it as done
4. The elected worker transitions the batch to PHJ_BATCH_SCAN phase
5. Checks if another process has set skip_unmatched flag (early termination optimization)
6. If proceeding, calls ExecPrepHashTableForUnmatched() to set up local scan state

The wait-free approach prevents deadlocks that could occur if workers blocked waiting for the barrier transition, since processes in PHJ_BATCH_PROBE phase have already begun emitting tuples.

## Parameters / Member Variables
- `hjstate`: Hash join state containing the hash table and current batch information

## Dependencies
- Functions called/Symbols referenced:
  - [HashJoinState](../H/HashJoinState.md) (struct type)
  - [HashJoinTable](../H/HashJoinTable.md) (struct type)
  - ParallelHashJoinBatch (struct type)
  - PHJ_BATCH_PROBE (barrier phase constant)
  - BarrierPhase (barrier state query function)
  - BarrierArriveAndDetachExceptLast (barrier coordination function)
  - [sts_end_parallel_scan](../s/sts_end_parallel_scan.md) (shared tuple store cleanup)
  - dsa_pointer_atomic (dynamic shared memory type)
  - PHJ_BATCH_SCAN (barrier phase constant)
  - [ExecHashTableDetachBatch](ExecHashTableDetachBatch.md) (batch cleanup function)
  - [ExecPrepHashTableForUnmatched](ExecPrepHashTableForUnmatched.md) (local state setup)
- Called from (representative examples):
  - [ExecHashJoinImpl](ExecHashJoinImpl.md)

## Notes and Other Information
- Returns true if this worker should perform the unmatched scan, false otherwise
- Uses wait-free election to avoid deadlock in parallel barrier coordination
- Only one worker per batch performs unmatched scanning to avoid duplicate results
- Non-elected workers can continue working on other available batches
- Handles the skip_unmatched optimization where scanning may be bypassed entirely
- Tracks space usage statistics normally handled by ExecHashTableDetachBatch()
- Essential for correctness of parallel RIGHT JOIN and FULL OUTER JOIN operations