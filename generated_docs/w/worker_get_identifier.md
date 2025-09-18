# worker_get_identifier

## Location
src/backend/utils/sort/tuplesort.c: 3019 - 3046

## Overview
Assigns and returns a unique ordinal identifier for a worker process in parallel tuplesort operations.

## Definition
```c
static int worker_get_identifier(Tuplesortstate *state)
```

## Detailed Description
This function provides a thread-safe mechanism for assigning unique worker identifiers during parallel tuple sorting operations. It uses a mutex-protected counter to ensure that each worker process receives a distinct, gapless identifier. The assignment order is not deterministic and should not matter for correctness. The identifiers are used internally by the sorting system and have no relation to ParallelWorkerNumber, following the convention of using -1 for non-worker processes (leader and serial processes).

## Parameters / Member Variables
- `state`: Pointer to the Tuplesortstate structure representing the current tuple sort operation

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire
  - SpinLockRelease
  - WORKER (macro)
  - Tuplesortstate
  - Sharedsort
- Called from (representative examples):
  - [tuplesort_begin_common](../t/tuplesort_begin_common.md)

## Notes and Other Information
- Function is marked as static, indicating internal use within the tuplesort module
- Uses mutex locking to ensure thread-safe identifier assignment
- Worker identifiers must be distinct and gapless as required by logtape.c
- Follows PostgreSQL convention of using -1 for non-worker processes
- The assignment order is deliberately undefined and should not be relied upon by callers
- Only callable for worker processes as verified by the WORKER() assertion