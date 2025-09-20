# date2timestamptz_opt_overflow

## Location
[src/backend/utils/adt/date.c:624-703](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L624-L703)

## Overview
Converts a DateADT value to a TimestampTz (timestamp with time zone) with optional overflow handling, incorporating timezone offset calculations.

## Definition

```c
struct pg_tm tt,
			   *tm = &tt;
```
## Detailed Description
The  function promotes a PostgreSQL date to a timestamp with time zone, incorporating timezone offset calculations. Unlike the regular timestamp conversion, this function must consider the session timezone to determine the appropriate UTC offset. It converts the date to a broken-down time structure, determines the timezone offset for midnight on that date, and adjusts the final timestamp accordingly. The function includes comprehensive overflow checking both before and after timezone adjustment, since timezone offsets can push values beyond valid timestamp ranges.

## Parameters / Member Variables
- : The DateADT input value to be converted to timestamp with time zone
- : Optional pointer to integer for overflow detection; if NULL, function throws errors on overflow; if not NULL, receives overflow indicator (0=no overflow, +1=positive overflow, -1=negative overflow)

## Dependencies
- Functions called/Symbols referenced:
  - DATE_IS_NOBEGIN: Checks for negative infinity date
  - DATE_IS_NOEND: Checks for positive infinity date
  - TIMESTAMP_NOBEGIN: Sets result to negative infinity timestamp
  - TIMESTAMP_NOEND: Sets result to positive infinity timestamp
  - [j2date](../j/j2date.md): Converts Julian day to year/month/day components
  - DetermineTimeZoneOffset: Calculates timezone offset for given time and zone
  - TIMESTAMP_END_JULIAN: Upper boundary constant for timestamp range
  - POSTGRES_EPOCH_JDATE: PostgreSQL epoch reference date
  - USECS_PER_DAY: Microseconds per day conversion constant
  - USECS_PER_SEC: Microseconds per second conversion constant
  - IS_VALID_TIMESTAMP: Validates timestamp is within supported range
  - MIN_TIMESTAMP: Lower boundary constant for timestamp range
  - session_timezone: Current session timezone setting
- Called from (representative examples):
  - [date2timestamptz](date2timestamptz.md): Wrapper function without overflow handling
  - [date_cmp_timestamptz_internal](date_cmp_timestamptz_internal.md): Date-timestamptz comparison function
  - PG_RETURN_TIMETZADT_P: Header macro usage

## Notes and Other Information
- Performs two-stage overflow checking: before timezone adjustment and after
- Unlike date2timestamp_opt_overflow, this function can produce negative overflow due to timezone adjustments
- Converts date to midnight (00:00:00) in the session timezone before applying UTC offset
- Timezone offset calculation considers historical timezone changes and daylight saving time rules
- More complex than regular timestamp conversion due to timezone considerations
- Essential for operations requiring timezone-aware date-timestamp interoperability