# timesub

## Location
src/timezone/localtime.c: 1414 - 1538

## Overview
Converts a timestamp to broken-down time fields, accounting for timezone offset and leap seconds.

## Definition

```c
static struct pg_tm *
timesub(const pg_time_t *timep, int32 offset,
		const struct state *sp, struct pg_tm *tmp)
```
## Detailed Description
The  function is a core timezone conversion routine that breaks down a Unix timestamp into calendar components (year, month, day, hour, minute, second, etc.). It handles timezone offsets, leap second corrections, and date calculations across year boundaries. The function performs complex arithmetic to convert seconds since epoch into human-readable date/time components while properly handling leap years and leap seconds.

The function operates in several phases:
1. Applies leap second corrections based on the timezone state
2. Converts the timestamp to days and remaining seconds
3. Iteratively adjusts for year boundaries and leap years
4. Calculates day of week, day of year, and calendar date
5. Breaks down remaining seconds into hours, minutes, and seconds

## Parameters / Member Variables
- : Pointer to the timestamp (seconds since Unix epoch) to convert
- : Timezone offset in seconds to apply to the timestamp
- : Pointer to timezone state structure containing leap second and transition information
- : Pointer to pg_tm structure to populate with the broken-down time

## Dependencies
- Functions called/Symbols referenced:
  - increment_overflow
  - leaps_thru_end_of
  - isleap
  - EPOCH_YEAR, SECSPERDAY, DAYSPERLYEAR, TM_YEAR_BASE (constants)
  - pg_time_t, pg_tm, lsinfo (types)
- Called from (representative examples):
  - localsub
  - gmtsub

## Notes and Other Information
- Returns NULL and sets errno to EOVERFLOW if the timestamp is out of representable range
- Handles positive leap seconds by setting tm_sec to 60 when a leap second occurs
- Uses careful overflow checking throughout to prevent integer overflow
- The function is static and used internally within the timezone subsystem
- Critical for PostgreSQL's timezone conversion functionality