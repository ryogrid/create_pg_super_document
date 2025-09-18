# timestamp_lt_date

## Location
[src/backend/utils/adt/date.c:925-933](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L925-L933)

## Overview
Compares a timestamp value against a date value and returns true if the timestamp is less than the date.

## Definition
```c
Datum timestamp_lt_date(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the < (less than) operator for comparing a timestamp (left operand) with a date (right operand). It extracts the timestamp and date values from the function arguments, then delegates to the internal comparison function `date_cmp_timestamp_internal()`. The function returns true when the comparison result is greater than zero, which indicates that the date is greater than the timestamp (i.e., the timestamp is less than the date).

Note that the comparison logic checks if the result is > 0 because `date_cmp_timestamp_internal` takes dateVal as the first parameter, so a positive result means date > timestamp, which translates to timestamp < date.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: Timestamp value (the timestamp to compare)
  - Argument 1: DateADT value (the date to compare against)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMESTAMP
  - PG_GETARG_DATEADT
  - [date_cmp_timestamp_internal](../d/date_cmp_timestamp_internal.md)
  - PG_RETURN_BOOL
- Called from (representative examples):
  - No direct callers found (likely called through SQL operator infrastructure)

## Notes and Other Information
- This function handles plain timestamp (without timezone) to date comparison, not timestamptz
- The comparison result is inverted because `date_cmp_timestamp_internal` compares (date, timestamp) but we want (timestamp < date)
- Returns true when the timestamp represents a date that comes before the given date
- Part of the cross-type comparison functions for dates in PostgreSQL
- The actual comparison logic is handled by `date_cmp_timestamp_internal()` which converts the date to timestamp format before comparison
- Located in src/backend/utils/adt/date.c:925-933