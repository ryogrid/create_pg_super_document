# worker_nomergeruns

## Location
[src/backend/utils/sort/tuplesort.c:3085-3106](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesort.c#L3085-L3106)

## Overview
Dumps memory tuples in a worker process without performing any merging operations when no merging is required.

## Definition
```c
static void worker_nomergeruns(Tuplesortstate *state)
```

## Detailed Description
This function serves as an alternative to mergeruns() for worker processes when the sorting operation doesn't require any merge phase. It's used when a worker has completed its sorting work and has exactly one output run that doesn't need to be merged with other runs. The function simply assigns the destination tape as the result tape and then calls worker_freeze_result_tape() to make the results available to the leader process for final coordination.

## Parameters / Member Variables
- `state`: Pointer to the Tuplesortstate structure representing this worker's tuple sort operation

## Dependencies
- Functions called/Symbols referenced:
  - [worker_freeze_result_tape](worker_freeze_result_tape.md)
  - WORKER (macro)
  - Tuplesortstate
- Called from (representative examples):
  - tuplesort_performsort

## Notes and Other Information
- Function is marked as static, indicating internal use within the tuplesort module
- Only callable by worker processes as verified by the WORKER() assertion
- Used as an optimization when no merging phase is needed (nOutputRuns == 1)
- Requires that no result tape has been set yet (result_tape == NULL)
- Directly assigns destTape as the result tape before freezing
- Part of the parallel tuplesort optimization path for simple cases
- Always calls worker_freeze_result_tape() to coordinate with the leader process