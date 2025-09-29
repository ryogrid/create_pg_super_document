# get_timeout_start_time

## Location
[src/backend/utils/misc/timeout.c:813-826](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/timeout.c#L813-L826)

## Overview
Returns the timestamp when the specified timeout was most recently activated, providing timing information for timeout management.

## Definition
```c
TimestampTz get_timeout_start_time(TimeoutId id)
```

## Detailed Description
This function retrieves the start time of a timeout, which records when the timeout was last enabled or activated. The start time is preserved across timeout events and is not reset when the timeout fires, avoiding potential race conditions that could occur if the value were modified during signal handling.

The function returns a TimestampTz value representing the absolute time when the timeout was activated. If a timeout has never been activated in the current process, the function returns 0. This persistent behavior ensures that timing information remains available for diagnostic and management purposes even after timeout events occur.

## Parameters / Member Variables
- `id`: TimeoutId specifying which timeout's start time to retrieve

## Dependencies
- Functions called/Symbols referenced:
  - [TimeoutId](../T/TimeoutId.md) (timeout identifier type)
  - TimestampTz (timestamp with timezone type)
- Called from (representative examples):
  - [ProcSleep](../P/ProcSleep.md) (process sleep/wait operations, called twice)
  - [DisableTimeoutParams](../D/DisableTimeoutParams.md) (macro wrapper)

## Notes and Other Information
- Returns 0 if the timeout has never been activated in the current process
- Start time is not reset when timeout fires, preventing race conditions
- Provides absolute timestamp information useful for timeout duration calculations
- Used primarily in lock management and process synchronization contexts
- The persistent nature of start_time values helps maintain consistent timing information
- No validation is performed on the TimeoutId parameter

## Simplified Source

```c
TimestampTz
get_timeout_start_time(TimeoutId id)
{
    // Return the start time for the specified timeout
    // Returns 0 if timeout has never been activated
    return all_timeouts[id].start_time;
}
```