# pg_localtime

## Location
src/timezone/localtime.c: 1344 - 1356

## Overview
The `pg_localtime` function converts a timestamp to local time representation according to a specified timezone, returning broken-down time components in a `pg_tm` structure.

## Definition
```c
struct pg_tm *pg_localtime(const pg_time_t *timep, const pg_tz *tz)
```

## Detailed Description
The `pg_localtime` function is PostgreSQL's timezone-aware version of the standard C library's `localtime` function. It takes a timestamp and a timezone specification and converts the timestamp to the corresponding local time in that timezone. The function handles timezone transitions, daylight saving time changes, and historical timezone data. It delegates the actual conversion work to the `localsub` function, which performs the complex timezone calculations including binary search through timezone transition times and proper handling of timezone abbreviations and UTC offsets.

## Parameters / Member Variables
- `timep`: Pointer to a `pg_time_t` value representing the timestamp to convert (typically seconds since Unix epoch)
- `tz`: Pointer to a `pg_tz` structure containing the timezone definition and state information

## Dependencies
- Functions called/Symbols referenced:
  - localsub
  - pg_time_t
  - pg_tz
  - pg_tm
- Called from (representative examples):
  - str_time
  - build_backup_content
  - timeofday
  - timestamp2tm
  - get_formatted_log_time
  - logfile_getname

## Notes and Other Information
This function is fundamental to PostgreSQL's timestamp handling and is used throughout the system for converting timestamps to local time representations. It properly handles edge cases such as times during DST transitions, times outside the range of available timezone data (using extrapolation), and invalid times. The function returns a pointer to a static `pg_tm` structure, so the result should be used immediately or copied if persistence is needed. Unlike the standard `localtime` function, this version is thread-safe when used with PostgreSQL's timezone infrastructure.