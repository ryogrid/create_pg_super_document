# BarrierArriveAndDetach

## Location
[src/backend/storage/ipc/barrier.c:203-212](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/barrier.c#L203-L212)

## Overview
Arrives at a barrier and immediately detaches from it without waiting, allowing a participant to signal arrival and leave the synchronization group atomically.

## Definition

```c
bool
BarrierArriveAndDetach(Barrier *barrier)
```
## Detailed Description
BarrierArriveAndDetach provides a mechanism for a participant to signal its arrival at a synchronization point while simultaneously removing itself from the barrier. This is a wrapper function that calls  with the arrival flag set.

The function's behavior depends on the barrier state:
- If other participants are still waiting and this detaching participant was the last one they were waiting for, the barrier phase advances and waiting participants are released
- If no other participants are waiting, the barrier phase still advances to maintain synchronization semantics
- The participant count is decremented and the caller is no longer considered part of the barrier

This operation is atomic and thread-safe, ensuring consistent barrier state during concurrent access.

## Parameters / Member Variables
- : Pointer to the Barrier structure from which to arrive and detach

## Dependencies
- Functions called/Symbols referenced:
  - BarrierDetachImpl
  - Barrier (struct type)
- Called from (representative examples):
  - ExecHashTableDetachBatch
  - ExecHashTableDetach

## Notes and Other Information
- This function is only valid for dynamic barriers (not static_party barriers)
- Returns true if this was the last participant to detach from the barrier (participant count becomes zero)
- The 'arrive' semantics ensure that detaching still counts as participating in the current synchronization phase
- Used primarily in parallel hash operations when a worker needs to leave the synchronization group
- The underlying BarrierDetachImpl handles the complex logic of phase advancement and participant notification
- Cannot be used with static party barriers - will trigger assertions if attempted