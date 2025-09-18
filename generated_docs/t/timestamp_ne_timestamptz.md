# timestamp_ne_timestamptz

## Location
src/backend/utils/adt/timestamp.c: 2355 - 2363

## Overview
This function implements the not-equal comparison operator (<>) between a timestamp without timezone and a timestamptz (timestamp with timezone) value.

## Definition
```c
Datum timestamp_ne_timestamptz(PG_FUNCTION_ARGS)
```

## Detailed Description
The function compares a timestamp (without timezone) value with a timestamptz (with timezone) value to determine if they are not equal. It extracts the two input arguments using PostgreSQL's function argument macros and delegates the actual comparison logic to the internal `timestamp_cmp_timestamptz_internal` function. The function returns true if the values are not equal (comparison result != 0), and false if they are equal.

This function is part of PostgreSQL's timestamp comparison operator family and handles cross-type comparisons between timestamp and timestamptz data types.

## Parameters / Member Variables
- `timestampVal`: The timestamp (without timezone) value from the first argument (PG_GETARG_TIMESTAMP(0))
- `dt2`: The timestamptz (with timezone) value from the second argument (PG_GETARG_TIMESTAMPTZ(1))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMESTAMP (macro for extracting timestamp argument)
  - PG_GETARG_TIMESTAMPTZ (macro for extracting timestamptz argument) 
  - timestamp_cmp_timestamptz_internal (internal comparison function)
  - PG_RETURN_BOOL (macro for returning boolean result)
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- Located in src/backend/utils/adt/timestamp.c:2355-2363
- This function implements the SQL <> operator for timestamp <> timestamptz comparisons
- The actual comparison logic is delegated to timestamp_cmp_timestamptz_internal which handles timezone conversions and comparisons
- Returns a Datum containing a boolean value that can be used in SQL expressions