# injection_wait

## Location
[src/test/modules/injection_points/injection_points.c:201-267](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/injection_points/injection_points.c#L201-L267)

## Overview
A callback function that implements a blocking wait mechanism for injection points, allowing test scenarios to synchronize and coordinate execution flow between different processes or threads.

## Definition
void injection_wait(const char *name, const void *private_data)

## Detailed Description
The `injection_wait` function provides a sophisticated synchronization mechanism for PostgreSQL's injection point testing framework. When triggered, it causes the calling process to block and wait on a condition variable until explicitly awakened by `injection_points_wakeup`. The function manages a shared memory structure that tracks active waiters, uses custom wait events for monitoring, and implements a robust slot-based system to handle multiple concurrent waiting injection points. This mechanism is essential for creating deterministic test scenarios where precise timing and coordination between different database operations is required.

## Parameters / Member Variables
- `name`: The name identifier of the injection point that will wait
- `private_data`: A pointer to an InjectionPointCondition structure containing filtering criteria

## Dependencies
- Functions called/Symbols referenced:
  - [injection_init_shmem](injection_init_shmem.md)
  - [injection_point_allowed](injection_point_allowed.md)
  - [WaitEventInjectionPointNew](../W/WaitEventInjectionPointNew.md)
  - [strlcpy](../s/strlcpy.md)
  - [ConditionVariablePrepareToSleep](../C/ConditionVariablePrepareToSleep.md)
  - [ConditionVariableSleep](../C/ConditionVariableSleep.md)
  - [ConditionVariableCancelSleep](../C/ConditionVariableCancelSleep.md)
  - [InjectionPointCondition](../I/InjectionPointCondition.md)
  - INJ_MAX_WAIT
  - INJ_NAME_MAXLEN
- Called from (representative examples):
  - No direct callers found (used as callback function name string in injection_points_attach)

## Notes and Other Information
- Implements a slot-based waiting system with a maximum of INJ_MAX_WAIT concurrent waiters
- Creates custom wait events using the injection point name for better observability in pg_stat_activity
- Uses spinlocks to protect shared memory access when managing waiter slots
- Will throw an ERROR if no free slot is available for waiting
- Automatically cleans up its slot when awakened or when the wait is cancelled
- Essential for creating race-condition-free tests in concurrent scenarios
- Part of the injection_points test module and works in conjunction with injection_points_wakeup

## Simplified Source

```c
void injection_wait(const char *name, const void *private_data) {
    uint32 old_wait_counts = 0;
    int index = -1;
    uint32 injection_wait_event = 0;
    InjectionPointCondition *condition = (InjectionPointCondition *) private_data;

    // Initialize shared memory if needed
    if (inj_state == NULL)
        injection_init_shmem();

    // Check if injection point is allowed
    if (!injection_point_allowed(condition))
        return;

    // Create custom wait event for monitoring
    injection_wait_event = WaitEventInjectionPointNew(name);

    // Find free slot and register this injection point
    SpinLockAcquire(&inj_state->lock);
    for (int i = 0; i < INJ_MAX_WAIT; i++) {
        if (inj_state->name[i][0] == '\0') {
            index = i;
            strlcpy(inj_state->name[i], name, INJ_NAME_MAXLEN);
            old_wait_counts = inj_state->wait_counts[i];
            break;
        }
    }
    SpinLockRelease(&inj_state->lock);

    // Error if no free slot available
    if (index < 0)
        elog(ERROR, "could not find free slot for wait of injection point %s", name);

    // Sleep until awakened by wait count change
    ConditionVariablePrepareToSleep(&inj_state->wait_point);
    for (;;) {
        uint32 new_wait_counts;

        SpinLockAcquire(&inj_state->lock);
        new_wait_counts = inj_state->wait_counts[index];
        SpinLockRelease(&inj_state->lock);

        if (old_wait_counts != new_wait_counts)
            break;
        ConditionVariableSleep(&inj_state->wait_point, injection_wait_event);
    }
    ConditionVariableCancelSleep();

    // Clean up: remove injection point from waiters
    SpinLockAcquire(&inj_state->lock);
    inj_state->name[index][0] = '\0';
    SpinLockRelease(&inj_state->lock);
}
```