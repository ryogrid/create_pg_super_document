# _brin_parallel_heapscan

## Location
src/backend/access/brin/brin.c: 2569 - 2609

## Overview
Coordinates completion of parallel heap scanning for BRIN index building by waiting for all worker processes to finish and collecting tuple statistics.

## Definition
```c
static double _brin_parallel_heapscan(BrinBuildState *state)
```

## Detailed Description
This function implements the leader's coordination logic for parallel heap scanning in BRIN index building:

1. **Wait for worker completion**: Uses a condition variable and spinlock-protected shared state to wait for all participating worker processes (including the leader if it participates) to complete their portion of the heap scan
2. **Statistics collection**: Once all workers are done, copies the aggregated tuple counts from shared memory into the leader's build state
3. **Synchronization management**: Uses PostgreSQL's condition variable mechanism to sleep while waiting, avoiding busy-waiting

The function runs in the leader process and blocks until all parallel workers have completed their heap scanning work. It serves as a synchronization point in the parallel BRIN building process.

## Parameters / Member Variables
- `state`: Pointer to BrinBuildState containing the leader information and build statistics that will be updated with final tuple counts

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire/SpinLockRelease (mutex protection)
  - [ConditionVariableSleep](../C/ConditionVariableSleep.md) (wait for workers)
  - [ConditionVariableCancelSleep](../C/ConditionVariableCancelSleep.md) (cleanup wait state)
  - [BrinShared](../B/BrinShared.md) (shared state structure)
- Called from (representative examples):
  - [_brin_parallel_merge](_brin_parallel_merge.md) (during parallel merge phase)

## Notes and Other Information
- Returns the total number of heap tuples scanned across all workers
- Uses WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN for wait event tracking
- The function polls shared state in a loop protected by spinlocks
- Copies both reltuples (heap tuples) and indtuples (index tuples) statistics
- Part of the coordination mechanism between leader and worker processes
- The leader may also participate as a worker, in which case it waits for itself and all other workers