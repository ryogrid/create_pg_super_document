# TimestampDifferenceMilliseconds

## Location
src/backend/utils/adt/timestamp.c: 1766 - 1789

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
  - pg_sub_s64_overflow (for safe subtraction with overflow detection)
  - INT64CONST (for 64-bit integer constants)
- Called from (representative examples):
  - LogCheckpointEnd
  - recoveryApplyDelay
  - WaitForWALToBecomeAvailable
  - do_analyze_rel
  - DetermineSleepTime
  - WalReceiverMain
  - WalSndComputeSleeptime

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