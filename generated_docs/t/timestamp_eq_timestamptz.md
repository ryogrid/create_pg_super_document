# timestamp_eq_timestamptz

## Location
[src/backend/utils/adt/timestamp.c:2346-2354](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L2346-L2354)

## Overview
The timestamp_eq_timestamptz function tests equality between a timestamp (without timezone) and a timestamptz (with timezone) value.

## Definition

```c
Datum
timestamp_eq_timestamptz(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the equality operator (=) for cross-type comparisons between timestamp and timestamptz data types. It extracts both timestamp arguments from the function call information, then delegates to timestamp_cmp_timestamptz_internal to perform the actual comparison with proper timezone handling. The function returns true if the comparison result is 0 (indicating equality), and false otherwise. This enables SQL expressions like 'timestamp_col = timestamptz_col' to work correctly across different timestamp types.

## Parameters / Member Variables
- : The timestamp value (without timezone) extracted from the first argument
- : The timestamptz value (with timezone) extracted from the second argument

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMESTAMP (extracts timestamp argument)
  - PG_GETARG_TIMESTAMPTZ (extracts timestamptz argument) 
  - [timestamp_cmp_timestamptz_internal](timestamp_cmp_timestamptz_internal.md) (performs the comparison)
  - PG_RETURN_BOOL (returns boolean result)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is typically called through PostgreSQL's SQL operator interface rather than directly
- Part of PostgreSQL's cross-type comparison operator family for timestamp types
- The actual comparison logic is handled by timestamp_cmp_timestamptz_internal, which this function wraps
- Returns a PostgreSQL Datum containing a boolean value indicating equality
- Enables seamless comparison between timestamp types with different timezone representations in SQL queries