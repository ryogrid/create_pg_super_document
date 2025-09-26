# enable_timeout_at

## Location
[src/backend/utils/misc/timeout.c:607-629](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/timeout.c#L607-L629)

## Overview
Enables a timeout to fire at a specific absolute timestamp, providing precise control over when the timeout should trigger.

## Definition

```c
void
enable_timeout_at(TimeoutId id, TimestampTz fin_time)
```
## Detailed Description
This function schedules a timeout to fire at an exact moment in time specified by fin_time. Unlike relative timeout functions that calculate the firing time from "now", this function allows scheduling based on a predetermined absolute timestamp. This is particularly useful when coordinating timeouts across multiple operations or when the timeout needs to align with external timing requirements.

The function follows the standard timeout setup pattern: disabling alarm interrupts for atomic configuration, setting up the timeout through the internal enable_timeout mechanism with a 0 delay (indicating no repetition), and rescheduling the system alarm.

## Parameters / Member Variables
- `id`: TimeoutId identifying which timeout handler to enable
- `fin_time`: TimestampTz specifying the exact timestamp when the timeout should fire

## Dependencies
- Functions called/Symbols referenced:
  - disable_alarm
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)  
  - [enable_timeout](enable_timeout.md)
  - [schedule_alarm](../s/schedule_alarm.md)
- Called from (representative examples):
  - [DisableTimeoutParams](../D/DisableTimeoutParams.md)

## Notes and Other Information
- Designed for cases where timeout calculation is based on a reference point other than the current time
- More efficient than enable_timeout_after() when you already have the target timestamp, as it avoids redundant GetCurrentTimestamp() calls
- The delay parameter passed to enable_timeout is 0, indicating this is a one-time timeout, not periodic
- Provides precise timestamp-based timeout scheduling for time-critical operations
- Part of PostgreSQL's comprehensive timeout management system for handling various timing scenarios