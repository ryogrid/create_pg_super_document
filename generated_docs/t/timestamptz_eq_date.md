# timestamptz_eq_date

## Location
src/backend/utils/adt/date.c: 970 - 978

## Overview
Compares a timestamp with timezone value with a date value to determine if they are equal.

## Definition
```c
Datum timestamptz_eq_date(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the equality comparison operator between a timestamp with timezone (timestamptz) and a date. It extracts a timestamptz and a date from the function arguments, then uses the internal comparison function `date_cmp_timestamptz_internal` to perform the comparison. The function returns true if the timestamptz is equal to the date, false otherwise.

The comparison is performed by delegating to `date_cmp_timestamptz_internal(dateVal, dt1)` and checking if the result equals 0, which indicates equality between the date and the timestamp with timezone.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: PostgreSQL function call context containing:
  - Argument 0: `TimestampTz dt1` - The timestamp with timezone value to compare
  - Argument 1: `DateADT dateVal` - The date value to compare against

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_TIMESTAMPTZ` - Extracts timestamp with timezone argument
  - `PG_GETARG_DATEADT` - Extracts date argument  
  - `[date_cmp_timestamptz_internal](../d/date_cmp_timestamptz_internal.md)` - Performs the actual comparison
  - `PG_RETURN_BOOL` - Returns boolean result
- Called from (representative examples):
  - No direct references found (likely called via SQL operator system)

## Notes and Other Information
- This function is typically invoked through PostgreSQL's SQL operator system when using the '=' operator between timestamptz and date types
- The comparison logic is implemented in `date_cmp_timestamptz_internal` which handles timezone conversion and comparison details
- Unlike the timestamp functions above, this function works with timezone-aware timestamps (TimestampTz)
- Part of PostgreSQL's date/time ADT (Abstract Data Type) implementation in src/backend/utils/adt/date.c