# date_ge_timestamptz

## Location
[src/backend/utils/adt/date.c:889-897](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L889-L897)

## Overview
Compares a date value against a timestamptz value and returns true if the date is greater than or equal to the timestamptz.

## Definition

```c
Datum
date_ge_timestamptz(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the >= operator for comparing a date (left operand) with a timestamptz (right operand). It extracts the date and timestamptz values from the function arguments, then delegates to the internal comparison function . The function returns true if the comparison result is greater than or equal to zero.

The function is part of PostgreSQL's date/time comparison infrastructure and handles the complexities of comparing different temporal data types by converting them to a common representation before comparison.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Argument 0: DateADT value (the date to compare)
  - Argument 1: TimestampTz value (the timestamptz to compare against)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_DATEADT
  - PG_GETARG_TIMESTAMPTZ
  - [date_cmp_timestamptz_internal](date_cmp_timestamptz_internal.md)
  - PG_RETURN_BOOL
- Called from (representative examples):
  - No direct callers found (likely called through SQL operator infrastructure)

## Notes and Other Information
- This function is typically invoked through the SQL >= operator when comparing date and timestamptz types
- The actual comparison logic is handled by  which properly handles timezone conversion and edge cases like infinity values
- Returns a PostgreSQL Datum boolean value wrapped by PG_RETURN_BOOL macro
- Located in src/backend/utils/adt/date.c:889-897