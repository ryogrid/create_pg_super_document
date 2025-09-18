# timestamp_gt_timestamptz

## Location
src/backend/utils/adt/timestamp.c: 2373 - 2381

## Overview
This function implements the greater-than comparison operator (>) between a timestamp without timezone and a timestamptz (timestamp with timezone) value.

## Definition
```c
Datum timestamp_gt_timestamptz(PG_FUNCTION_ARGS)
```

## Detailed Description
The function compares a timestamp (without timezone) value with a timestamptz (with timezone) value to determine if the first value is greater than the second. It extracts the two input arguments using PostgreSQL's function argument macros and delegates the actual comparison logic to the internal `timestamp_cmp_timestamptz_internal` function. The function returns true if the timestamp value is greater than the timestamptz value (comparison result > 0), and false otherwise.

This function is part of PostgreSQL's timestamp comparison operator family and handles cross-type comparisons between timestamp and timestamptz data types for ordering operations.

## Parameters / Member Variables
- `timestampVal`: The timestamp (without timezone) value from the first argument (PG_GETARG_TIMESTAMP(0))
- `dt2`: The timestamptz (with timezone) value from the second argument (PG_GETARG_TIMESTAMPTZ(1))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMESTAMP (macro for extracting timestamp argument)
  - PG_GETARG_TIMESTAMPTZ (macro for extracting timestamptz argument) 
  - [timestamp_cmp_timestamptz_internal](timestamp_cmp_timestamptz_internal.md) (internal comparison function)
  - PG_RETURN_BOOL (macro for returning boolean result)
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- Located in src/backend/utils/adt/timestamp.c:2373-2381
- This function implements the SQL > operator for timestamp > timestamptz comparisons
- The actual comparison logic is delegated to timestamp_cmp_timestamptz_internal which handles timezone conversions and comparisons
- Returns a Datum containing a boolean value that can be used in SQL expressions and ordering operations
- Essential for range queries and sorting operations involving mixed timestamp types