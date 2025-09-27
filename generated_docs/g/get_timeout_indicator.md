# get_timeout_indicator

## Location
[src/backend/utils/misc/timeout.c:793-812](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/timeout.c#L793-L812)

## Overview
Returns and optionally resets a timeout's fired indicator flag, which signals whether the timeout has expired.

## Definition
```c
bool get_timeout_indicator(TimeoutId id, bool reset_indicator)
```

## Detailed Description
This function provides a safe mechanism to check whether a timeout has fired (expired) and optionally reset the indicator flag. The indicator is set to true when a timeout expires and remains set until explicitly reset. This persistent nature allows the system to detect timeout events even if they occur asynchronously.

The function includes race condition protection by only resetting the indicator when it's currently true, preventing the loss of timeout notifications that might occur between the check and reset operations. If the indicator is false, it is never modified, ensuring that timeout events occurring during the function call are not missed.

## Parameters / Member Variables
- `id`: TimeoutId specifying which timeout's indicator to check
- `reset_indicator`: Boolean flag controlling whether to reset the indicator to false when returning true

## Dependencies
- Functions called/Symbols referenced:
  - [TimeoutId](../T/TimeoutId.md) (timeout identifier type)
- Called from (representative examples):
  - [ProcessInterrupts](../P/ProcessInterrupts.md) (interrupt processing in postgres.c, called twice)
  - [DisableTimeoutParams](../D/DisableTimeoutParams.md) (macro wrapper)

## Notes and Other Information
- Provides race condition protection by only resetting indicators when they are currently true
- The persistent indicator mechanism ensures timeout events are not lost in asynchronous environments
- Used primarily in interrupt processing to detect and respond to timeout events
- Returns true if the timeout indicator was set, false otherwise
- The reset_indicator parameter allows callers to control whether they want a one-shot check or a persistent indicator
- No validation is performed on the TimeoutId parameter

## Simplified Source

```c
// Simplified version of get_timeout_indicator
bool get_timeout_indicator(TimeoutId id, bool reset_indicator) {
    // Check if timeout has fired
    if (all_timeouts[id].indicator) {
        // Reset indicator if requested
        if (reset_indicator) {
            all_timeouts[id].indicator = false;
        }
        return true;
    }
    return false;
}
```

Key simplifications made:
- Removed detailed comments about race conditions
- Added clear comments for the main logic branches
- Maintained essential race condition protection
- Preserved all functionality and parameter behavior