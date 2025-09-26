# reschedule_timeouts

## Location
[src/backend/utils/misc/timeout.c:540-559](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/timeout.c#L540-L559)

## Overview
Reschedules any pending SIGALRM interrupt, primarily used during error recovery to restore timeout functionality after signal handling interruption.

## Definition
```c
void reschedule_timeouts(void)
```

## Detailed Description
The `reschedule_timeouts` function provides a recovery mechanism for the PostgreSQL timeout system when SIGALRM signals may have been lost or interrupted during error handling. This situation commonly occurs when query cancellation or other error conditions cause execution to longjmp out of the `handle_sig_alarm` signal handler before it can complete its processing.

The function performs these operations:
1. **Initialization Check**: Safely handles calls made before timeout system initialization
2. **Alarm Disabling**: Temporarily disables alarms to ensure atomic rescheduling
3. **Conditional Rescheduling**: If active timeouts exist, reschedules the next SIGALRM based on current time

This function is particularly important during transaction abort and error recovery scenarios where normal timeout processing might be disrupted. It ensures that timeout functionality is restored even after exceptional control flow changes.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - disable_alarm: Disables alarm system temporarily for safety
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md): Gets current system timestamp for scheduling
  - [schedule_alarm](../s/schedule_alarm.md): Schedules the next SIGALRM interrupt
- Called from (representative examples):
  - [AbortTransaction](../A/AbortTransaction.md): Transaction abort cleanup
  - [AbortSubTransaction](../A/AbortSubTransaction.md): Subtransaction abort cleanup

## Notes and Other Information
- Designed specifically for error recovery scenarios where SIGALRM handling was interrupted
- Safe to call before timeout system initialization (returns early if not initialized)
- Not necessary to call if other timeout functions (enable_timeout, disable_timeout) are used in the same error handling code path, as they handle rescheduling internally
- Primarily used during transaction and subtransaction abort processing
- Does not affect the timeout registration or handler functions, only reschedules pending alarms
- Critical for maintaining timeout functionality across PostgreSQL error handling and recovery mechanisms