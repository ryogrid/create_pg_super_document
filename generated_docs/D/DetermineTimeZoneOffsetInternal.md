# DetermineTimeZoneOffsetInternal

## Location
[src/backend/utils/adt/datetime.c:1607-1745](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datetime.c#L1607-L1745)

## Overview
The core implementation for timezone offset calculation that handles DST transitions, ambiguous times, and edge cases while also returning the computed UTC time value.

## Definition
```c
static int DetermineTimeZoneOffsetInternal(struct pg_tm *tm, pg_tz *tzp, pg_time_t *tp)
```

## Detailed Description
DetermineTimeZoneOffsetInternal performs the complex work of determining timezone offsets while handling all the edge cases that arise with DST transitions. It converts the given local date/time to a UTC timestamp and determines the appropriate GMT offset, carefully handling situations where local times are either invalid (during spring-forward transitions) or ambiguous (during fall-back transitions).

The function uses pg_next_dst_boundary to find DST transition points and implements sophisticated logic to resolve ambiguous times. For invalid times during spring-forward, it prefers the "before" interpretation; for ambiguous times during fall-back, it prefers the "after" interpretation. This approach provides consistent behavior regardless of whether standard or daylight time is considered "normal" for the zone.

The implementation avoids using the system's mktime() function for performance and reliability reasons, instead performing direct calculations using PostgreSQL's internal time representation.

## Parameters / Member Variables
- `tm`: Pointer to pg_tm struct with input date/time fields, will have tm_isdst set on output
- `tzp`: Pointer to timezone definition containing DST rules and offset information  
- `tp`: Output parameter that receives the calculated UTC time as pg_time_t

## Dependencies
- Functions called/Symbols referenced:
  - IS_VALID_JULIAN (macro to validate Julian date range)
  - date2j (convert date to Julian day number)
  - pg_next_dst_boundary (find next DST transition boundary)
  - UNIX_EPOCH_JDATE, SECS_PER_DAY, MINS_PER_HOUR, SECS_PER_MINUTE (time constants)
- Called from (representative examples):
  - DetermineTimeZoneOffset (public wrapper function)
  - DetermineTimeZoneAbbrevOffset (timezone abbreviation handling)

## Notes and Other Information
- Returns GMT offset in seconds (negative of the timezone's UTC offset)
- Sets tp to 0 and returns 0 for out-of-range dates instead of throwing errors
- Assumes all GMT offsets are less than 24 hours and DST boundaries are at least 48 hours apart
- Handles overflow detection for pg_time_t arithmetic operations
- Implements consistent disambiguation rules for invalid/ambiguous times during DST transitions
- Significantly faster than system mktime() implementations
- Static function not exposed globally to prevent misuse of overflow behavior