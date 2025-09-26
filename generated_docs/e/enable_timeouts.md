# enable_timeouts

## Location
src/backend/utils/misc/timeout.c: 630 - 684

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
  - GetCurrentTimestamp
  - TimestampTzPlusMilliseconds
  - enable_timeout
  - schedule_alarm
- Types referenced:
  - EnableTimeoutParams
  - TimeoutId
  - TMPARAM_AFTER
  - TMPARAM_AT
  - TMPARAM_EVERY
- Called from (representative examples):
  - ResolveRecoveryConflictWithLock
  - ResolveRecoveryConflictWithBufferPin
  - ProcSleep
  - DisableTimeoutParams

## Notes and Other Information
- Optimizes performance by calling GetCurrentTimestamp() and setitimer() only once regardless of the number of timeouts
- Supports three timeout scheduling modes: relative delay, absolute timestamp, and periodic intervals
- Each timeout type is handled differently in the switch statement to accommodate various timing requirements
- Error handling includes validation of timeout types with appropriate error messages
- Particularly useful in scenarios like lock resolution and process sleeping where multiple timeouts coordinate different aspects of the operation
- Part of PostgreSQL's advanced timeout management system designed for high-performance concurrent operations