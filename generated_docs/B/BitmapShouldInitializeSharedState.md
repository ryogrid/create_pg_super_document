# BitmapShouldInitializeSharedState

## Location
[src/backend/executor/nodeBitmapHeapscan.c:784-816](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeBitmapHeapscan.c#L784-L816)

## Overview
This function determines which process should become the leader in a parallel bitmap heap scan and initialize the shared TIDBitmap state.

## Definition

```c
static bool
BitmapShouldInitializeSharedState(ParallelBitmapHeapState *pstate)
```
## Detailed Description
BitmapShouldInitializeSharedState implements a leader election mechanism for parallel bitmap heap scans. When multiple worker processes participate in a parallel bitmap scan, exactly one process needs to populate the shared TIDBitmap while others wait. This function uses atomic state transitions and condition variables to coordinate this process.

The function operates through a spin-lock protected state machine:
- The first process to see BM_INITIAL state becomes the leader and transitions the state to BM_INPROGRESS
- Subsequent processes finding BM_INPROGRESS state will block on a condition variable
- The leader process returns true and proceeds to initialize the bitmap
- Worker processes return false and wait for the leader to complete initialization

## Parameters / Member Variables
- : Pointer to ParallelBitmapHeapState structure containing shared state for parallel bitmap scanning, including mutex, condition variable, and current state

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire
  - SpinLockRelease
  - [ConditionVariableSleep](../C/ConditionVariableSleep.md)
  - [ConditionVariableCancelSleep](../C/ConditionVariableCancelSleep.md)
  - [ParallelBitmapHeapState](../P/ParallelBitmapHeapState.md) (structure)
  - SharedBitmapState (enum type)
  - BM_INITIAL (enum value)
  - BM_INPROGRESS (enum value)
- Called from (representative examples):
  - [BitmapHeapNext](BitmapHeapNext.md)

## Notes and Other Information
This function is critical for avoiding race conditions in parallel bitmap scans. The spin-lock ensures atomic state transitions, while the condition variable provides efficient blocking/wakeup semantics. The function implements a standard leader-follower pattern commonly used in PostgreSQL's parallel query execution framework.

## Simplified Source

```c
static bool
BitmapShouldInitializeSharedState(ParallelBitmapHeapState *pstate)
{
    SharedBitmapState state;

    while (1) {
        // Atomically check and claim leadership if available
        SpinLockAcquire(&pstate->mutex);
        state = pstate->state;
        if (pstate->state == BM_INITIAL)
            pstate->state = BM_INPROGRESS;  // Become leader
        SpinLockRelease(&pstate->mutex);

        // Exit if bitmap is done, or if we're the leader
        if (state != BM_INPROGRESS)
            break;

        // Wait for the leader to wake us up
        ConditionVariableSleep(&pstate->cv, WAIT_EVENT_PARALLEL_BITMAP_SCAN);
    }

    ConditionVariableCancelSleep();

    return (state == BM_INITIAL);  // True if we became leader
}
```