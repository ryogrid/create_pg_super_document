# BarrierParticipants

## Location
[src/backend/storage/ipc/barrier.c:281-299](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/barrier.c#L281-L299)

## Overview
BarrierParticipants is a debugging utility function that returns a snapshot of the current number of participants attached to a barrier synchronization point.

## Definition
```c
int BarrierParticipants(Barrier *barrier)
```

## Detailed Description
BarrierParticipants provides a thread-safe way to query the current number of participants attached to a barrier. Unlike other barrier functions, this function acquires and releases the barrier's mutex to ensure an atomic read of the participant count. The function is specifically intended for debugging purposes, as indicated in the source code comments.

The function takes a snapshot of the participant count at the moment the mutex is acquired, but this value may change immediately after the function returns if other processes are concurrently attaching to or detaching from the barrier.

## Parameters / Member Variables
- `barrier`: Pointer to the Barrier structure whose participant count is being queried

## Dependencies
- Functions called/Symbols referenced:
  - [Barrier](Barrier.md) (struct type)
  - SpinLockAcquire (implicitly through barrier->mutex)
  - SpinLockRelease (implicitly through barrier->mutex)
- Called from (representative examples):
  - No current references found in the codebase (debugging function)

## Notes and Other Information
- This function is explicitly marked as being "for debugging purposes only" in the source comments
- Uses mutex locking to ensure atomic access to the participant count, unlike BarrierPhase which reads without locking
- The returned value represents an instantaneous snapshot and may be outdated immediately after the function returns
- Currently unused in the main PostgreSQL codebase, suggesting it's primarily for development and debugging scenarios
- Located in src/backend/storage/ipc/barrier.c:281-299

## Simplified Source

```c
int BarrierParticipants(Barrier *barrier) {
    // Thread-safe read of current participant count for debugging
    SpinLockAcquire(&barrier->mutex);
    int participants = barrier->participants;
    SpinLockRelease(&barrier->mutex);

    return participants;
}
```