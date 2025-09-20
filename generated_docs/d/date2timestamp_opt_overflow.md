# date2timestamp_opt_overflow

## Location
[src/backend/utils/adt/date.c:564-607](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L564-L607)

## Overview
Converts a DateADT value to a Timestamp with optional overflow handling, allowing callers to detect and handle out-of-range conditions gracefully.

## Definition

```c
struct pg_tm tt,
			   *tm = &tt;
```
## Detailed Description
The  function promotes a PostgreSQL date to a timestamp with sophisticated overflow detection. It handles infinite date values by converting them to corresponding timestamp infinities. For finite dates, it performs range checking against timestamp boundaries and provides two modes of operation: error-throwing mode when overflow parameter is NULL, or overflow-reporting mode when a valid overflow pointer is provided. The conversion multiplies the date value (days since epoch) by microseconds per day to produce the timestamp.

## Parameters / Member Variables
- : The DateADT input value to be converted to timestamp
- : Optional pointer to integer for overflow detection; if NULL, function throws errors on overflow; if not NULL, receives overflow indicator (0=no overflow, +1=positive overflow, -1=negative overflow)

## Dependencies
- Functions called/Symbols referenced:
  - DATE_IS_NOBEGIN: Checks for negative infinity date
  - DATE_IS_NOEND: Checks for positive infinity date
  - TIMESTAMP_NOBEGIN: Sets result to negative infinity timestamp
  - TIMESTAMP_NOEND: Sets result to positive infinity timestamp
  - TIMESTAMP_END_JULIAN: Upper boundary constant for timestamp range
  - POSTGRES_EPOCH_JDATE: PostgreSQL epoch reference date
  - USECS_PER_DAY: Microseconds per day conversion constant
  - ereport: Error reporting function
- Called from (representative examples):
  - [date2timestamp](date2timestamp.md): Wrapper function without overflow handling
  - [date_cmp_timestamp_internal](date_cmp_timestamp_internal.md): Date-timestamp comparison function
  - PG_RETURN_TIMETZADT_P: Header macro usage

## Notes and Other Information
- Only checks upper boundary for overflow since dates and timestamps share the same lower bound (Julian day zero)
- Negative overflow (*overflow = -1) is theoretically impossible with current implementation
- Converts dates (days since 2000-01-01) to timestamps (microseconds since same epoch) via multiplication
- Provides flexible error handling strategy through optional overflow parameter
- Critical function for date-timestamp interoperability in PostgreSQL type system