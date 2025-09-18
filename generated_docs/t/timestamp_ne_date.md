# timestamp_ne_date

## Location
src/backend/utils/adt/date.c: 916 - 924

## Overview
Compares a timestamp value against a date value and returns true if they represent different dates (i.e., they are not equal).

## Definition
```c
Datum timestamp_ne_date(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the <> (not equal) operator for comparing a timestamp (left operand) with a date (right operand). It extracts the timestamp and date values from the function arguments, then delegates to the internal comparison function `date_cmp_timestamp_internal()`. The function returns true when the comparison result is not zero, indicating the timestamp and date represent different points in time when considering the date component.

This function is the logical complement of `timestamp_eq_date` and handles plain timestamp (without timezone) to date comparison. It is part of PostgreSQL's cross-type comparison infrastructure.

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
- Returns true when the timestamp and date represent different dates
- The actual comparison logic is handled by `date_cmp_timestamp_internal()` which converts the date to timestamp format before comparison
- Complement function to `timestamp_eq_date` - returns opposite boolean result
- Part of the cross-type comparison functions for dates in PostgreSQL
- Located in src/backend/utils/adt/date.c:916-924