# BarrierArriveAndWait

## Location
[src/backend/storage/ipc/barrier.c:125-202](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/barrier.c#L125-L202)

## Overview
Arrives at a barrier and waits for all other attached participants to arrive, implementing a synchronization point with leader election functionality.

## Definition

```c
bool
BarrierArriveAndWait(Barrier *barrier, uint32 wait_event_info)
```
## Detailed Description
BarrierArriveAndWait provides a synchronization mechanism where multiple backend processes can wait at a specific point until all participants have arrived. The function implements a two-phase synchronization protocol with leader election:

1. **Arrival Phase**: The calling backend increments the arrival counter and checks if it's the last participant to arrive
2. **Wait/Release Phase**: Either immediately returns as the elected leader (if last to arrive) or waits for the barrier to be released by the last participant

Key behaviors:
- Increments the barrier's phase counter when all participants have arrived
- Elects exactly one participant (returns true) while others return false
- The elected participant can perform serial work while others proceed
- Uses condition variables for efficient waiting with configurable wait event reporting
- Handles dynamic scenarios where participants may detach during waiting

## Parameters / Member Variables
- : Pointer to the initialized Barrier structure
- : Wait event identifier for pg_stat_activity reporting during sleep

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire/SpinLockRelease
  - [ConditionVariableBroadcast](../C/ConditionVariableBroadcast.md)
  - [ConditionVariablePrepareToSleep](../C/ConditionVariablePrepareToSleep.md)
  - [ConditionVariableSleep](../C/ConditionVariableSleep.md)
  - [ConditionVariableCancelSleep](../C/ConditionVariableCancelSleep.md)
  - [Barrier](Barrier.md) (struct type)
- Called from (representative examples):
  - [MultiExecParallelHash](../M/MultiExecParallelHash.md)
  - [ExecHashTableCreate](../E/ExecHashTableCreate.md)
  - [ExecParallelHashIncreaseNumBatches](../E/ExecParallelHashIncreaseNumBatches.md)
  - [ExecParallelHashIncreaseNumBuckets](../E/ExecParallelHashIncreaseNumBuckets.md)
  - [ExecHashJoinImpl](../E/ExecHashJoinImpl.md)
  - [ExecParallelHashJoinNewBatch](../E/ExecParallelHashJoinNewBatch.md)

## Notes and Other Information
- The caller must be attached to the barrier before calling this function
- Exactly one participant will be elected (return true) per phase, typically the last to arrive
- If a participant detaches while others are waiting, one of the awakened participants will be elected
- The function maintains phase coherence - the barrier phase can only be the current phase or the next phase
- Wait events allow monitoring of barrier waits in pg_stat_activity for performance analysis
- Primarily used in parallel hash join operations for coordinating parallel worker synchronization points

## Simplified Source

```c
bool
BarrierArriveAndWait(Barrier *barrier, uint32 wait_event_info)
{
    bool release = false;
    bool elected;
    int start_phase;
    int next_phase;

    // Atomically increment arrival count and check if we're the last
    SpinLockAcquire(&barrier->mutex);
    start_phase = barrier->phase;
    next_phase = start_phase + 1;
    ++barrier->arrived;

    if (barrier->arrived == barrier->participants)
    {
        // Last to arrive - elect as leader and advance phase
        release = true;
        barrier->arrived = 0;
        barrier->phase = next_phase;
        barrier->elected = next_phase;
    }
    SpinLockRelease(&barrier->mutex);

    // If we're the elected leader, wake up all waiters
    if (release)
    {
        ConditionVariableBroadcast(&barrier->condition_variable);
        return true;
    }

    // Otherwise wait for the barrier to advance
    elected = false;
    ConditionVariablePrepareToSleep(&barrier->condition_variable);

    for (;;)
    {
        SpinLockAcquire(&barrier->mutex);
        release = barrier->phase == next_phase;

        // Handle election if someone detached while we were waiting
        if (release && barrier->elected != next_phase)
        {
            barrier->elected = barrier->phase;
            elected = true;
        }
        SpinLockRelease(&barrier->mutex);

        if (release)
            break;

        ConditionVariableSleep(&barrier->condition_variable, wait_event_info);
    }

    ConditionVariableCancelSleep();
    return elected;
}
```

This simplified version shows the core barrier synchronization: atomically count arrivals, elect the last participant as leader, and coordinate waiting/waking using condition variables.