# timestamp_pl_interval

## Location
src/backend/utils/adt/timestamp.c: 3049 - 3165

## Overview
Adds an interval to a timestamp data type, handling both quantitative time and qualitative year/month/day units correctly.

## Definition


## Detailed Description
This function implements timestamp arithmetic by adding an interval to a timestamp. It handles the complexity of calendar arithmetic by processing different interval components separately:

1. **Month addition**: Increments the month component and adjusts for month overflow/underflow, handling end-of-month boundary conditions (e.g., Jan 31 + 1 month = Feb 28/29)
2. **Day addition**: Uses Julian day arithmetic to properly handle day overflow across month and year boundaries
3. **Time addition**: Adds the microsecond-precision time component directly

The function includes comprehensive infinity handling, treating combinations like "infinity - infinity" as errors since PostgreSQL timestamps have no NaN equivalent.

## Parameters / Member Variables
- Input parameter 0: Timestamp value via 
- Input parameter 1: Interval pointer via 
- Returns: A Datum containing the resulting timestamp

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMESTAMP, PG_GETARG_INTERVAL_P, PG_RETURN_TIMESTAMP
  - timestamp2tm, tm2timestamp (timestamp/tm conversion)
  - date2j, j2date (Julian date conversion)
  - pg_add_s32_overflow, pg_add_s64_overflow (overflow-safe arithmetic)
  - isleap (leap year detection)
  - IS_VALID_TIMESTAMP (range validation)
- Constants used:
  - MONTHS_PER_YEAR (12)
  - day_tab (days per month lookup table)
- Called from:
  - date_pl_interval (src/backend/utils/adt/date.c:1254)
  - timestamp_mi_interval (src/backend/utils/adt/timestamp.c:3174)
  - in_range_timestamp_interval (src/backend/utils/adt/timestamp.c:3865)
  - generate_series_timestamp (src/backend/utils/adt/timestamp.c:6571)

## Notes and Other Information
- Handles all infinity combinations correctly, preventing "infinity - infinity" scenarios
- Uses Julian day arithmetic for accurate day arithmetic across calendar boundaries
- Implements proper end-of-month handling (e.g., Jan 31 + 1 month = Feb 28/29, not Feb 31)
- Includes comprehensive overflow detection for all arithmetic operations
- The function follows PostgreSQL's standard SQL-callable function pattern
- Part of PostgreSQL's core temporal arithmetic system, used extensively in date/time operations