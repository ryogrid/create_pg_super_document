# worker_freeze_result_tape

## Location
[src/backend/utils/sort/tuplesort.c:3047-3084](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesort.c#L3047-L3084)

## Overview
Freezes a worker's result tape and makes it available to the leader process in parallel tuplesort operations.

## Definition
```c
static void worker_freeze_result_tape(Tuplesortstate *state)
```

## Detailed Description
This function is called by worker processes after they have completed their sorting work and determined their final result tape. It performs additional steps beyond a simple LogicalTapeFreeze() that are specifically required for parallel sorting operations. The function handles resource cleanup by freeing memory used for tuple storage, freezes the result tape to make it persistent, and updates shared state to notify the leader process that this worker's results are ready for merging. Each worker should produce exactly one final output run containing all tuples that were originally input to that worker.

## Parameters / Member Variables
- `state`: Pointer to the Tuplesortstate structure representing this worker's tuple sort operation

## Dependencies
- Functions called/Symbols referenced:
  - LogicalTapeFreeze
  - SpinLockAcquire
  - SpinLockRelease
  - [pfree](../p/pfree.md)
  - WORKER (macro)
  - Tuplesortstate
  - Sharedsort
  - TapeShare
- Called from (representative examples):
  - mergeruns
  - [worker_nomergeruns](worker_nomergeruns.md)

## Notes and Other Information
- Function is marked as static, indicating internal use within the tuplesort module
- Only callable by worker processes as verified by the WORKER() assertion
- Frees memory resources (memtuples) as workers no longer need them after freezing
- Updates shared worker finished count in a thread-safe manner using spinlocks
- Stores tape metadata in shared memory at the worker's designated index
- Essential for coordinating the transition from worker sorting to leader merging phase
- Each worker must have exactly one result tape when this function is called