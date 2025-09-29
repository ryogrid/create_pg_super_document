# enable_timeouts

## Location
[src/backend/utils/misc/timeout.c:630-684](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/timeout.c#L630-L684)

## Overview
Enables multiple timeouts simultaneously with different scheduling types, optimizing performance by reducing system calls when setting up multiple timeouts.

## Definition

```c
void
enable_timeouts(const EnableTimeoutParams *timeouts, int count)
```
## Detailed Description
This function provides an efficient way to enable multiple timeouts at once, avoiding repeated calls to GetCurrentTimestamp() and setitimer() that would occur when setting up timeouts individually. It supports three different timeout types through the EnableTimeoutParams structure: TMPARAM_AFTER (relative delay), TMPARAM_AT (absolute timestamp), and TMPARAM_EVERY (periodic intervals).

The function processes each timeout in the array according to its specified type, calculating the appropriate firing times and configuring each timeout through the internal enable_timeout mechanism. This batch processing approach is particularly beneficial when multiple timeouts need to be coordinated or when system performance is critical.

## Parameters / Member Variables
- `timeouts`: const EnableTimeoutParams* pointing to an array of timeout parameter structures
- `count`: int specifying the number of timeouts to enable from the array

## Dependencies
- Functions called/Symbols referenced:
  - disable_alarm
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - TimestampTzPlusMilliseconds
  - [enable_timeout](enable_timeout.md)
  - [schedule_alarm](../s/schedule_alarm.md)
- Types referenced:
  - EnableTimeoutParams
  - [TimeoutId](../T/TimeoutId.md)
  - TMPARAM_AFTER
  - TMPARAM_AT
  - TMPARAM_EVERY
- Called from (representative examples):
  - [ResolveRecoveryConflictWithLock](../R/ResolveRecoveryConflictWithLock.md)
  - [ResolveRecoveryConflictWithBufferPin](../R/ResolveRecoveryConflictWithBufferPin.md)
  - [ProcSleep](../P/ProcSleep.md)
  - [DisableTimeoutParams](../D/DisableTimeoutParams.md)

## Notes and Other Information
- Optimizes performance by calling GetCurrentTimestamp() and setitimer() only once regardless of the number of timeouts
- Supports three timeout scheduling modes: relative delay, absolute timestamp, and periodic intervals
- Each timeout type is handled differently in the switch statement to accommodate various timing requirements
- Error handling includes validation of timeout types with appropriate error messages
- Particularly useful in scenarios like lock resolution and process sleeping where multiple timeouts coordinate different aspects of the operation
- Part of PostgreSQL's advanced timeout management system designed for high-performance concurrent operations

## Simplified Source

```c
void
enable_timeouts(const EnableTimeoutParams *timeouts, int count)
{
    TimestampTz now;
    int i;

    // Disable interrupts for safety during setup
    disable_alarm();

    // Get current time once for all timeouts
    now = GetCurrentTimestamp();

    // Process each timeout configuration
    for (i = 0; i < count; i++)
    {
        TimeoutId id = timeouts[i].id;
        TimestampTz fin_time;

        // Handle different timeout types
        switch (timeouts[i].type)
        {
            case TMPARAM_AFTER:
                // Relative timeout (delay from now)
                fin_time = TimestampTzPlusMilliseconds(now, timeouts[i].delay_ms);
                enable_timeout(id, now, fin_time, 0);
                break;

            case TMPARAM_AT:
                // Absolute timeout (specific timestamp)
                enable_timeout(id, now, timeouts[i].fin_time, 0);
                break;

            case TMPARAM_EVERY:
                // Periodic timeout (repeating)
                fin_time = TimestampTzPlusMilliseconds(now, timeouts[i].delay_ms);
                enable_timeout(id, now, fin_time, timeouts[i].delay_ms);
                break;

            default:
                elog(ERROR, "unrecognized timeout type %d", (int) timeouts[i].type);
                break;
        }
    }

    // Activate the timer system
    schedule_alarm(now);
}
```