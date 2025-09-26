# timestamptz_pl_interval_internal

## Location
[src/backend/utils/adt/timestamp.c:3192-3323](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L3192-L3323)

## Overview
Internal function that adds an interval to a timestamp with timezone (timestamptz), handling timezone-aware calendar arithmetic properly.

## Definition

```c
struct pg_tm tt,
					   *tm = &tt;
```
## Detailed Description
This is the core implementation function for timestamptz-interval addition that handles timezone-aware arithmetic. Unlike the plain timestamp version, this function must account for timezone effects throughout the calculation:

1. **Timezone handling**: Uses the specified timezone (or session timezone if NULL) for all calendar arithmetic
2. **Month addition**: Converts to local time, adds months with end-of-month handling, then determines the correct timezone offset for the result
3. **Day addition**: Performs day arithmetic in local time using Julian dates, recalculating timezone offsets
4. **Time addition**: Adds microseconds directly to the UTC timestamp value

The function is critical for ensuring that calendar arithmetic respects timezone rules, including daylight saving time transitions, which can affect the final result.

## Parameters / Member Variables
- : The input TimestampTz (UTC-based timestamp with timezone)
- : Pointer to the Interval structure containing the values to add
- : Timezone to use for calendar calculations (NULL uses session timezone)
- Returns: TimestampTz result after timezone-aware addition

## Dependencies
- Functions called/Symbols referenced:
  - [timestamp2tm](timestamp2tm.md), tm2timestamp (timezone-aware timestamp/tm conversion)
  - [DetermineTimeZoneOffset](../D/DetermineTimeZoneOffset.md) (calculates timezone offset for given local time)
  - [date2j](../d/date2j.md), j2date (Julian date conversion for day arithmetic)
  - [pg_add_s32_overflow](../p/pg_add_s32_overflow.md), pg_add_s64_overflow (overflow-safe arithmetic)
  - isleap (leap year detection)
  - IS_VALID_TIMESTAMP (range validation)
- Global variables:
  - session_timezone (default timezone when attimezone is NULL)
- Constants used:
  - MONTHS_PER_YEAR (12)
  - day_tab (days per month lookup table)
- Called from:
  - [timestamptz_mi_interval_internal](timestamptz_mi_interval_internal.md) (src/backend/utils/adt/timestamp.c:3332)
  - [timestamptz_pl_interval](timestamptz_pl_interval.md) (src/backend/utils/adt/timestamp.c:3344)
  - [timestamptz_pl_interval_at_zone](timestamptz_pl_interval_at_zone.md) (src/backend/utils/adt/timestamp.c:3367)
  - [in_range_timestamptz_interval](../i/in_range_timestamptz_interval.md) (src/backend/utils/adt/timestamp.c:3826)
  - [generate_series_timestamptz_internal](../g/generate_series_timestamptz_internal.md) (src/backend/utils/adt/timestamp.c:6657)

## Notes and Other Information
- Static function - not directly exposed to SQL, only used internally
- Handles all infinity combinations correctly, preventing "infinity - infinity" scenarios
- More permissive Julian date range (allows -1) compared to plain timestamp version, accommodating timezone-dependent edge cases
- Recalculates timezone offsets after both month and day arithmetic to handle DST transitions correctly
- Critical for PostgreSQL's timezone-aware temporal arithmetic - ensures results respect local calendar rules
- The timezone parameter allows for cross-timezone calculations when needed