# timestamptz_cmp_date

## Location
src/backend/utils/adt/date.c: 1024 - 1038

## Overview
Compares a timestamptz value with a date value, returning an integer comparison result suitable for sorting operations.

## Definition
```c
Datum timestamptz_cmp_date(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the comparison operator for timestamptz and date data types. It extracts a timestamptz value and a date value from the function arguments, then delegates the actual comparison logic to `date_cmp_timestamptz_internal`. The result is negated to maintain proper ordering semantics when comparing a timestamptz against a date.

The function serves as a PostgreSQL internal function that can be called from SQL operations involving comparisons between timestamptz and date values. It handles the conversion and comparison logic needed to properly order these different but related temporal data types.

## Parameters / Member Variables
- `PG_GETARG_TIMESTAMPTZ(0)`: The timestamptz value to compare (first argument)
- `PG_GETARG_DATEADT(1)`: The date value to compare against (second argument)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_TIMESTAMPTZ` - Extracts timestamptz argument
  - `PG_GETARG_DATEADT` - Extracts date argument  
  - `date_cmp_timestamptz_internal` - Internal comparison function
  - `DateADT` - Date abstract data type
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- The function negates the result from `date_cmp_timestamptz_internal` to maintain proper comparison semantics
- Part of PostgreSQL's date/time comparison infrastructure
- Located in src/backend/utils/adt/date.c:1024-1038
- Returns an integer: negative if timestamptz < date, zero if equal, positive if timestamptz > date