# timestamp_age

## Location
[src/backend/utils/adt/timestamp.c:4247-4392](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L4247-L4392)

## Overview
Calculates the time difference between two timestamps while retaining year/month fields, producing an interval that preserves calendar semantics rather than absolute time spans.

## Definition
```c
Datum timestamp_age(PG_FUNCTION_ARGS)
```

## Detailed Description
This function computes the "age" or time difference between two timestamp values. Unlike simple timestamp subtraction, this function preserves year and month components in a way that reflects calendar arithmetic rather than absolute time differences. This means that the result accounts for variable month lengths and leap years.

The function performs several key operations:
1. Handles infinite timestamp values with appropriate error checking
2. Converts timestamps to broken-down time structures using timestamp2tm()
3. Performs field-by-field subtraction of time components
4. Handles negative field propagation (borrowing) across time units
5. Accounts for variable month lengths when borrowing days
6. Converts the result back to an interval using itm2interval()

The calculation is complex because it maintains calendar semantics - for example, the difference between Jan 31 and Mar 1 should be reported as 1 month and X days, accounting for February's actual length.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: PostgreSQL function argument macro containing:
  - Arg 0: Timestamp (first timestamp - "from" time)
  - Arg 1: Timestamp (second timestamp - "to" time)

## Dependencies
- Functions called/Symbols referenced:
  - Timestamp (timestamp data type)
  - Interval (interval data type)  
  - fsec_t (fractional seconds type)
  - [pg_itm](../p/pg_itm.md) (interval time structure)
  - [pg_tm](../p/pg_tm.md) (broken-down time structure)
  - PG_GETARG_TIMESTAMP (PostgreSQL macro)
  - PG_RETURN_INTERVAL_P (PostgreSQL macro)
  - TIMESTAMP_IS_NOBEGIN/TIMESTAMP_IS_NOEND (infinity check macros)
  - INTERVAL_NOBEGIN/INTERVAL_NOEND (infinity interval macros)
  - [timestamp2tm](timestamp2tm.md) (timestamp to broken-down time conversion)
  - [itm2interval](../i/itm2interval.md) (interval time structure to interval conversion)
  - Time constants: USECS_PER_SEC, SECS_PER_MINUTE, MINS_PER_HOUR, HOURS_PER_DAY, MONTHS_PER_YEAR
  - day_tab (days per month lookup table)
  - isleap (leap year check function)
  - ereport (error reporting function)
  - [palloc](../p/palloc.md) (memory allocation function)
- Called from (representative examples):
  - No direct references found (likely used through PostgreSQL's SQL function infrastructure)

## Notes and Other Information
- This function implements the AGE() SQL function for timestamp values
- The result is not an accurate absolute time span due to calendar arithmetic - year and month components lose absolute meaning once computed
- Handles infinite timestamps appropriately, treating "infinity - infinity" as an error
- The complex borrowing logic ensures proper handling of negative intermediate values
- Month length calculations account for leap years when borrowing days
- Sign handling ensures the result direction matches the timestamp comparison
- Error handling for out-of-range results and invalid timestamp values
- Related to timestamptz_age but operates on timestamp without timezone values