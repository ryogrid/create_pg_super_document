# timestamptz_age

## Location
src/backend/utils/adt/timestamp.c: 4393 - 4546

## Overview
Calculates the time difference between two timestamp with timezone values while retaining year/month fields, producing an interval that preserves calendar semantics rather than absolute time spans.

## Definition
```c
Datum timestamptz_age(PG_FUNCTION_ARGS)
```

## Detailed Description
This function computes the "age" or time difference between two timestamp with timezone (timestamptz) values. It is nearly identical to timestamp_age() but operates on timezone-aware timestamps. Like its counterpart, it preserves year and month components in a way that reflects calendar arithmetic rather than absolute time differences.

The function performs the same key operations as timestamp_age():
1. Handles infinite timestamp values with appropriate error checking
2. Converts timestamptz values to broken-down time structures using timestamp2tm() (which extracts timezone information)
3. Performs field-by-field subtraction of time components
4. Handles negative field propagation (borrowing) across time units  
5. Accounts for variable month lengths when borrowing days
6. Converts the result back to an interval using itm2interval()

A key aspect is that the function deliberately ignores timezone differences between the two input timestamps - it works with the local time components after timezone conversion, making the result independent of timezone offsets.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: PostgreSQL function argument macro containing:
  - Arg 0: TimestampTz (first timestamp with timezone - "from" time)
  - Arg 1: TimestampTz (second timestamp with timezone - "to" time)

## Dependencies
- Functions called/Symbols referenced:
  - TimestampTz (timestamp with timezone data type)
  - Interval (interval data type)
  - fsec_t (fractional seconds type)
  - pg_itm (interval time structure)
  - pg_tm (broken-down time structure)
  - PG_GETARG_TIMESTAMPTZ (PostgreSQL macro)
  - PG_RETURN_INTERVAL_P (PostgreSQL macro)
  - TIMESTAMP_IS_NOBEGIN/TIMESTAMP_IS_NOEND (infinity check macros)
  - INTERVAL_NOBEGIN/INTERVAL_NOEND (infinity interval macros)
  - timestamp2tm (timestamp to broken-down time conversion with timezone)
  - itm2interval (interval time structure to interval conversion)
  - Time constants: USECS_PER_SEC, SECS_PER_MINUTE, MINS_PER_HOUR, HOURS_PER_DAY, MONTHS_PER_YEAR
  - day_tab (days per month lookup table)
  - isleap (leap year check function)
  - ereport (error reporting function)
  - palloc (memory allocation function)
- Called from (representative examples):
  - No direct references found (likely used through PostgreSQL's SQL function infrastructure)

## Notes and Other Information
- This function implements the AGE() SQL function for timestamptz values
- Nearly identical to timestamp_age() but handles timezone-aware timestamps
- Timezone information is extracted during timestamp2tm() conversion but then ignored in the calculation
- The result represents calendar-based time difference, not absolute duration
- Handles infinite timestamps appropriately, treating "infinity - infinity" as an error
- Complex borrowing logic ensures proper handling of negative intermediate values
- Month length calculations account for leap years when borrowing days
- Sign handling ensures the result direction matches the timestamp comparison
- Error handling for out-of-range results and invalid timestamp values
- The comment "Note: we deliberately ignore any difference between tz1 and tz2" indicates timezone offsets don't affect the final interval calculation