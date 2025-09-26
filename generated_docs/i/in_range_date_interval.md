# in_range_date_interval

## Location
[src/backend/utils/adt/date.c:1039-1065](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L1039-L1065)

## Overview
Provides in_range support function for date data type, determining if a date value falls within a specified range defined by a base date and an interval offset.

## Definition
```c
Datum in_range_date_interval(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the in_range support functionality for PostgreSQL window functions operating on date values with interval offsets. It determines whether a given date value falls within a range defined by a base date and an interval offset.

The function works by promoting both the input date and base date to timestamp values (without time zone) and then delegating the actual range checking logic to `in_range_timestamp_interval`. This approach leverages the existing timestamp interval logic while providing the interface needed for date-based window operations.

The function supports both addition and subtraction of intervals from the base date, and can check for either "less than or equal" or "less than" comparisons, making it flexible for various window frame specifications.

## Parameters / Member Variables
- `PG_GETARG_DATEADT(0)`: The date value to test (`val`)
- `PG_GETARG_DATEADT(1)`: The base date for comparison (`base`)  
- `PG_GETARG_INTERVAL_P(2)`: The interval offset to apply (`offset`)
- `PG_GETARG_BOOL(3)`: Whether to subtract the interval instead of adding (`sub`)
- `PG_GETARG_BOOL(4)`: Whether to use "less than" instead of "less than or equal" (`less`)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_DATEADT` - Extracts date arguments
  - `PG_GETARG_INTERVAL_P` - Extracts interval argument
  - `PG_GETARG_BOOL` - Extracts boolean arguments
  - [date2timestamp](../d/date2timestamp.md) - Converts date to timestamp
  - `DirectFunctionCall5` - Direct function call mechanism
  - [in_range_timestamp_interval](in_range_timestamp_interval.md) - Timestamp interval range function
  - `[TimestampGetDatum](../T/TimestampGetDatum.md)`, `IntervalPGetDatum`, `BoolGetDatum` - Datum conversion functions
  - `DateADT`, `Interval`, `Timestamp` - Data type definitions
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Part of PostgreSQL's window function infrastructure for date ranges
- Converts dates to timestamps to reuse existing timestamp interval logic
- Located in src/backend/utils/adt/date.c:1039-1065
- Contains a TODO comment about potentially supporting out-of-range cases
- Used internally by PostgreSQL's window function processing for date-based frames with interval specifications