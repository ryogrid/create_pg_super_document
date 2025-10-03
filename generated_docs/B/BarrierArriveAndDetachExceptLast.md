# BarrierArriveAndDetachExceptLast

## Location
[src/backend/storage/ipc/barrier.c:213-235](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/barrier.c#L213-L235)

## Overview
Arrives at a barrier and detaches all but the last participant, implementing a "winner-takes-all" synchronization pattern where only one participant remains attached.

## Definition

```c
bool
BarrierArriveAndDetachExceptLast(Barrier *barrier)
```
## Detailed Description
BarrierArriveAndDetachExceptLast implements a specialized synchronization pattern where multiple participants arrive at a barrier, but only the last one remains attached while all others detach immediately. This creates a reduction pattern where many participants converge to a single remaining participant.

The function's behavior is deterministic based on timing:
- If there are multiple participants (> 1): The calling participant decrements the participant count and detaches, returning false
- If there is only one participant remaining (== 1): The participant becomes the "winner," the phase advances, and the function returns true

This operation is atomic and does not involve waiting or condition variable signaling, making it more efficient than full barrier synchronization when only one participant needs to continue.

## Parameters / Member Variables
- `*barrier`: Pointer to the Barrier structure to operate on
## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire/SpinLockRelease
  - [Barrier](Barrier.md) (struct type)
- Called from (representative examples):
  - [ExecParallelPrepHashTableForUnmatched](../E/ExecParallelPrepHashTableForUnmatched.md)
  - [ExecHashTableDetachBatch](../E/ExecHashTableDetachBatch.md)

## Notes and Other Information
- Returns true only for the last participant to call this function (the "winner")
- All other participants return false and are no longer attached to the barrier
- The phase is advanced only when the last participant is determined
- No condition variable signaling occurs as there are no waiting participants
- Used in parallel hash operations to elect a single worker to continue processing
- More efficient than BarrierArriveAndWait when only one participant needs to proceed
- The function maintains barrier state consistency without the overhead of waiting and notification
- Requires at least one participant to be attached when called (asserts participants >= 1)

## Simplified Source

```c
bool BarrierArriveAndDetachExceptLast(Barrier *barrier)
{
    SpinLockAcquire(&barrier->mutex);

    // If not the last participant, detach and return false
    if (barrier->participants > 1) {
        --barrier->participants;
        SpinLockRelease(&barrier->mutex);
        return false;
    }

    // Last participant - advance phase and return true
    Assert(barrier->participants == 1);
    ++barrier->phase;
    SpinLockRelease(&barrier->mutex);

    return true;
}
```