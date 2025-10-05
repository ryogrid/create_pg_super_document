# date_cmp_timestamptz

## Location
[src/backend/utils/adt/date.c:898-906](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L898-L906)

## Overview
Performs a three-way comparison between a date value and a timestamptz value, returning an integer indicating their relative ordering.

## Definition
```c
Datum date_cmp_timestamptz(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements a comparison function for ordering date and timestamptz values. It extracts the date and timestamptz values from the function arguments and delegates to `date_cmp_timestamptz_internal()` to perform the actual comparison. The function returns an integer result: negative if the date is less than the timestamptz, zero if they are equal, and positive if the date is greater than the timestamptz.

This function is typically used by PostgreSQL's sorting and indexing infrastructure when ordering mixed date/timestamptz data, and serves as the foundation for other comparison operators.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: DateADT value (the date to compare)
  - Argument 1: TimestampTz value (the timestamptz to compare against)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_DATEADT
  - PG_GETARG_TIMESTAMPTZ
  - [date_cmp_timestamptz_internal](date_cmp_timestamptz_internal.md)
  - PG_RETURN_INT32
- Called from (representative examples):
  - No direct callers found (likely called through SQL comparison infrastructure)

## Notes and Other Information
- Returns < 0 if date < timestamptz, 0 if equal, > 0 if date > timestamptz
- The actual comparison logic is handled by `date_cmp_timestamptz_internal()` which manages timezone conversion and edge cases
- This function serves as the basis for other comparison operators (=, <, >, <=, >=, <>)
- Located in src/backend/utils/adt/date.c:898-906

## Simplified Source

```c
Datum
date_cmp_timestamptz(PG_FUNCTION_ARGS)
{
    // Extract date and timestamptz arguments
    DateADT dateVal = PG_GETARG_DATEADT(0);
    TimestampTz timestamptz = PG_GETARG_TIMESTAMPTZ(1);

    // Return three-way comparison result (-1, 0, or 1)
    PG_RETURN_INT32(date_cmp_timestamptz_internal(dateVal, timestamptz));
}
```