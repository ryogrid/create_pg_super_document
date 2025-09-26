# get_timeout_finish_time

## Location
[src/backend/utils/misc/timeout.c:827-830](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/timeout.c#L827-L830)

## Overview
Returns the timestamp when the specified timeout is scheduled to fire or most recently was due to fire.

## Definition
```c
TimestampTz get_timeout_finish_time(TimeoutId id)
```

## Detailed Description
This function retrieves the finish time (expiration time) of a timeout, which represents the absolute time when the timeout is scheduled to expire or when it most recently expired. The finish time is calculated when a timeout is enabled and represents the deadline for the timeout event.

Like the start time, the finish time is preserved across timeout events and is not reset when the timeout fires. This design prevents race conditions that could occur if the signal handler modified the value while application code was attempting to read it. The persistent nature of the finish time allows for accurate timeout duration calculations and diagnostics.

## Parameters / Member Variables
- `id`: TimeoutId specifying which timeout's finish time to retrieve

## Dependencies
- Functions called/Symbols referenced:
  - TimeoutId (timeout identifier type)  
  - TimestampTz (timestamp with timezone type)
- Called from (representative examples):
  - ProcessInterrupts (interrupt processing for timeout handling)
  - DisableTimeoutParams (macro wrapper)

## Notes and Other Information
- Returns 0 if the timeout has never been activated in the current process
- Finish time is not reset when timeout fires, preventing race conditions
- Represents the scheduled expiration time rather than actual firing time
- Used for timeout duration calculations and deadline management
- Provides absolute timestamp information useful for determining remaining time
- The persistent nature helps maintain consistent timing information across timeout events
- No validation is performed on the TimeoutId parameter