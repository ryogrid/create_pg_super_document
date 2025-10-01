# BarrierDetachImpl

## Location
[src/backend/storage/ipc/barrier.c:300-333](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/barrier.c#L300-L333)

## Overview
BarrierDetachImpl is the core implementation function for detaching from a barrier, providing both simple detachment and arrive-and-detach functionality with proper synchronization and phase advancement logic.

## Definition
```c
static inline bool BarrierDetachImpl(Barrier *barrier, bool arrive)
```

## Detailed Description
BarrierDetachImpl is the internal implementation that handles the complex logic of detaching from a barrier synchronization point. It manages two scenarios: simple detachment (used by BarrierDetach) and arrive-and-detach operations (used by BarrierArriveAndDetach). 

The function performs several critical operations atomically under mutex protection:
1. Decrements the participant count
2. Determines if waiting participants should be released based on whether this was the last awaited participant
3. Advances the barrier phase when appropriate
4. Tracks whether this was the final participant to detach

The `arrive` parameter controls whether the detaching process is also considered to have "arrived" at the barrier, which affects phase advancement logic when no other participants are waiting.

## Parameters / Member Variables
- `barrier`: Pointer to the Barrier structure from which to detach
- `arrive`: Boolean indicating whether this detachment should also count as an arrival for phase advancement purposes

## Dependencies
- Functions called/Symbols referenced:
  - [Barrier](Barrier.md) (struct type)
  - [ConditionVariableBroadcast](../C/ConditionVariableBroadcast.md)
  - SpinLockAcquire (implicitly through barrier->mutex)
  - SpinLockRelease (implicitly through barrier->mutex)
  - Assert (macro)
- Called from (representative examples):
  - [BarrierArriveAndDetach](BarrierArriveAndDetach.md)
  - [BarrierDetach](BarrierDetach.md)

## Notes and Other Information
- This is a static inline function, meaning it's only visible within the barrier.c file and optimized for inlining
- Contains an assertion that the barrier is not configured for static party mode
- Uses condition variable broadcasting to wake up waiting participants when the phase advances
- The function's dual behavior (simple detach vs arrive-and-detach) is controlled by the `arrive` parameter
- Returns true if this participant was the last to detach, enabling cleanup logic in callers
- Handles the complex logic of determining when to advance the barrier phase and release waiting participants
- Located in src/backend/storage/ipc/barrier.c:300-333

## Simplified Source

```c
static inline bool BarrierDetachImpl(Barrier *barrier, bool arrive) {
    bool release = false;
    bool last;

    // Verify this is not a static party barrier
    Assert(!barrier->static_party);

    // Safely update barrier state under lock
    SpinLockAcquire(&barrier->mutex);
    Assert(barrier->participants > 0);
    --barrier->participants;

    // Determine if we should advance phase and release waiting participants
    if ((arrive || barrier->participants > 0) &&
        barrier->arrived == barrier->participants) {
        release = true;
        barrier->arrived = 0;
        ++barrier->phase;
    }

    // Check if this was the last participant
    last = (barrier->participants == 0);
    SpinLockRelease(&barrier->mutex);

    // Wake up any waiting participants if phase advanced
    if (release)
        ConditionVariableBroadcast(&barrier->condition_variable);

    return last;
}
```