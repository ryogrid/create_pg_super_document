# TimestampDifference

## Location
src/backend/utils/adt/timestamp.c: 1730 - 1765

## Overview
Converts the difference between two timestamps into separate integer seconds and microseconds components, primarily designed for calculating wait timeouts.

## Definition
void TimestampDifference(TimestampTz start_time, TimestampTz stop_time, long *secs, int *microsecs)

## Detailed Description
This utility function calculates the time difference between two TimestampTz values and separates the result into whole seconds and remaining microseconds. The function is specifically designed for use with system calls like select(2) that require timeout values in this format. It expects both input timestamps to be ordinary finite timestamps, typically obtained from GetCurrentTimestamp().

The function handles edge cases gracefully: if the start_time is greater than or equal to stop_time (meaning we've already passed the target time), it returns zero for both seconds and microseconds. Otherwise, it performs integer division and modulo operations to separate the difference into seconds and microseconds components.

## Parameters / Member Variables
- `start_time`: The earlier timestamp (TimestampTz) - typically the current time when setting up a wait
- `stop_time`: The later timestamp (TimestampTz) - typically the target time when a wait should end  
- `secs`: Output parameter (long*) - receives the number of whole seconds in the time difference
- `microsecs`: Output parameter (int*) - receives the remaining microseconds after whole seconds are accounted for

## Dependencies
- Functions called/Symbols referenced:
  - USECS_PER_SEC (constant for microseconds per second conversion)
- Called from (representative examples):
  - heap_vacuum_rel
  - launcher_determine_sleep  
  - ProcSleep
  - check_log_duration
  - pgstat_report_activity
  - schedule_alarm

## Notes and Other Information
- The function is located in src/backend/utils/adt/timestamp.c:1730-1765
- Returns void - results are provided through output parameters secs and microsecs
- Expects start_time <= stop_time; returns zeros if this condition is violated
- Designed specifically for select(2) and similar system calls requiring timeout in seconds/microseconds format
- Uses USECS_PER_SEC constant for conversion between timestamp internal format and seconds/microseconds
- Widely used throughout PostgreSQL for timeout calculations in various subsystems including vacuum, autovacuum, process sleeping, and statistics reporting
- Input timestamps should be finite values (not infinity or -infinity)