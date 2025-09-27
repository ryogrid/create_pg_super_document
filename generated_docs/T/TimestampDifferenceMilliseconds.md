# TimestampDifferenceMilliseconds

## Location
[src/backend/utils/adt/timestamp.c:1766-1789](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L1766-L1789)

## Overview
Converts the difference between two timestamps into integer milliseconds, specifically designed for wait timeout calculations with proper overflow handling and rounding.

## Definition
long TimestampDifferenceMilliseconds(TimestampTz start_time, TimestampTz stop_time)

## Detailed Description
This function calculates the time difference between two TimestampTz values and returns the result in milliseconds as a long integer. It is specifically designed for use with WaitLatch() and similar functions that require timeout values in milliseconds. The function includes sophisticated overflow protection, clamping results to INT_MAX milliseconds to match the limitations of the target wait functions.

The function handles several important edge cases: it returns zero for negative elapsed time (when start_time >= stop_time), detects and handles overflow conditions when dealing with timestamp infinities, and implements proper rounding behavior by rounding up fractional milliseconds to ensure that waits don't terminate prematurely.

## Parameters / Member Variables
- `start_time`: The earlier timestamp (TimestampTz) - typically the current time when setting up a wait
- `stop_time`: The later timestamp (TimestampTz) - typically the target time when a wait should end

## Dependencies
- Functions called/Symbols referenced:
  - [pg_sub_s64_overflow](../p/pg_sub_s64_overflow.md) (for safe subtraction with overflow detection)
  - INT64CONST (for 64-bit integer constants)
- Called from (representative examples):
  - [LogCheckpointEnd](../L/LogCheckpointEnd.md)
  - [recoveryApplyDelay](../r/recoveryApplyDelay.md)
  - [WaitForWALToBecomeAvailable](../W/WaitForWALToBecomeAvailable.md)
  - [do_analyze_rel](../d/do_analyze_rel.md)
  - [DetermineSleepTime](../D/DetermineSleepTime.md)
  - [WalReceiverMain](../W/WalReceiverMain.md)
  - [WalSndComputeSleeptime](../W/WalSndComputeSleeptime.md)

## Notes and Other Information
- The function is located in src/backend/utils/adt/timestamp.c:1766-1789
- Returns long integer representing milliseconds (clamped to INT_MAX)
- Handles timestamp infinities gracefully without failing
- Rounds up fractional milliseconds using (diff + 999) / 1000 to avoid premature timeout
- Designed specifically for WaitLatch() and related PostgreSQL wait functions
- Includes overflow protection using pg_sub_s64_overflow()
- Returns zero if start_time >= stop_time (past the target time)
- Maximum return value is INT_MAX milliseconds to match WaitLatch() constraints
- Widely used throughout PostgreSQL for timeout calculations in WAL processing, replication, checkpoints, and various background processes

## Simplified Source

```c
// Simplified version of TimestampDifferenceMilliseconds
long TimestampDifferenceMilliseconds(TimestampTz start_time, TimestampTz stop_time) {
    TimestampTz time_diff;

    // Return zero if already past stop time
    if (start_time >= stop_time) {
        return 0;
    }

    // Calculate difference with overflow protection
    if (pg_sub_s64_overflow(stop_time, start_time, &time_diff)) {
        return (long) INT_MAX;  // Overflow occurred
    }

    // Check if result would exceed INT_MAX milliseconds
    if (time_diff >= (INT_MAX * 1000 - 999)) {
        return (long) INT_MAX;  // Clamp to maximum
    }

    // Convert microseconds to milliseconds, rounding up
    return (long) ((time_diff + 999) / 1000);
}
```

Key simplifications made:
- Renamed variable `diff` to `time_diff` for clarity
- Added descriptive comments for each logical step
- Simplified overflow check comment to be more direct
- Made the rounding-up behavior more explicit in comments
- Focused on the core algorithm: validate inputs, handle overflow, convert units
- Preserved all essential logic including edge case handling