# timestamp_eq_date

## Location
[src/backend/utils/adt/date.c:907-915](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L907-L915)

## Overview
Compares a timestamp value against a date value and returns true if they represent the same date (i.e., they are equal).

## Definition
```c
Datum timestamp_eq_date(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the = (equality) operator for comparing a timestamp (left operand) with a date (right operand). It extracts the timestamp and date values from the function arguments, then delegates to the internal comparison function `date_cmp_timestamp_internal()`. The function returns true only if the comparison result equals zero, indicating the timestamp and date represent the same point in time when considering only the date component.

Note that this compares a plain timestamp (without timezone) against a date, which is different from the timestamptz comparison functions. The function is part of PostgreSQL's cross-type comparison infrastructure.

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
- The actual comparison logic is handled by `date_cmp_timestamp_internal()` which converts the date to timestamp format before comparison
- Returns true only when both values represent the exact same date
- Part of the cross-type comparison functions for dates in PostgreSQL
- Located in src/backend/utils/adt/date.c:907-915

## Simplified Source

```c
Datum
timestamp_eq_date(PG_FUNCTION_ARGS)
{
    // Extract timestamp and date arguments
    Timestamp timestamp = PG_GETARG_TIMESTAMP(0);
    DateADT dateVal = PG_GETARG_DATEADT(1);

    // Return true if timestamp equals date (comparison result == 0)
    PG_RETURN_BOOL(date_cmp_timestamp_internal(dateVal, timestamp) == 0);
}
```