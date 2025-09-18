# ConditionVariableSleep

## Location
[src/backend/storage/lmgr/condition_variable.c:96-111](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/condition_variable.c#L96-L111)

## Overview
Puts the current process to sleep until the condition variable is signaled, serving as the primary blocking mechanism in condition variable-based synchronization patterns.

## Definition
```c
void ConditionVariableSleep(ConditionVariable *cv, uint32 wait_event_info)
```

## Detailed Description
ConditionVariableSleep is the core function for waiting on a condition variable. It blocks the current process until the condition variable is signaled by another process. This function is designed to be used within predicate loops where the process repeatedly tests an exit condition and sleeps if the condition is not met.

The function is implemented as a simple wrapper around ConditionVariableTimedSleep with no timeout (-1). This design provides a clean interface for indefinite waiting while leveraging the full functionality of the timed sleep implementation.

The typical usage pattern involves calling ConditionVariablePrepareToSleep (optionally), then entering a loop that tests the exit condition and calls ConditionVariableSleep if needed, followed by ConditionVariableCancelSleep to clean up.

## Parameters / Member Variables
- `cv`: Pointer to the ConditionVariable to wait on
- `wait_event_info`: Value from WaitEventXXX enums that appears in pg_stat_activity for monitoring purposes

## Dependencies
- Functions called/Symbols referenced:
  - [ConditionVariableTimedSleep](ConditionVariableTimedSleep.md) (performs the actual timed sleep operation)
- Called from (representative examples):
  - [_brin_parallel_heapscan](../b/_brin_parallel_heapscan.md)
  - [_bt_parallel_seize](../b/_bt_parallel_seize.md)
  - [_bt_parallel_heapscan](../b/_bt_parallel_heapscan.md)
  - [GetMultiXactIdMembers](../G/GetMultiXactIdMembers.md)
  - [BitmapShouldInitializeSharedState](../B/BitmapShouldInitializeSharedState.md)
  - Checkpointer signal handling
  - replorigin_state_clear
  - [ReplicationSlotAcquire](../R/ReplicationSlotAcquire.md)
  - [InvalidatePossiblyObsoleteSlot](../I/InvalidatePossiblyObsoleteSlot.md)
  - [ShutdownWalRcv](../S/ShutdownWalRcv.md)
  - WaitIO
  - BarrierArriveAndWait
  - [injection_wait](../i/injection_wait.md)

## Notes and Other Information
- Should be used in predicate loops that test exit conditions
- Implements indefinite waiting (no timeout) by calling ConditionVariableTimedSleep with -1
- The wait_event_info parameter enables monitoring through pg_stat_activity
- Typically preceded by ConditionVariablePrepareToSleep for efficiency
- Must be followed by ConditionVariableCancelSleep to clean up the wait state
- Used extensively in parallel operations, replication, buffer management, and synchronization barriers
- The process will be awakened by ConditionVariableBroadcast or ConditionVariableSignal calls