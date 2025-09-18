# timestamptz_cmp_timestamp

## Location
[src/backend/utils/adt/timestamp.c:2463-2482](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L2463-L2482)

## Overview
Compares a timestamp with timezone (timestamptz) value with a plain timestamp value and returns an integer indicating their relative ordering (-1, 0, or 1).

## Definition
```c
Datum timestamptz_cmp_timestamp(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the three-way comparison (spaceship operator) between a timestamptz (timestamp with timezone) and a plain timestamp. It extracts the two arguments from the PostgreSQL function call framework and uses the internal comparison function `timestamp_cmp_timestamptz_internal` to perform the actual comparison. The result is negated to maintain proper comparison semantics where negative values indicate the first argument is less than the second, zero indicates equality, and positive values indicate the first argument is greater than the second.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: TimestampTz value (timestamp with timezone)
  - Argument 1: Timestamp value (plain timestamp without timezone)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMESTAMPTZ (macro to extract timestamptz argument)
  - PG_GETARG_TIMESTAMP (macro to extract timestamp argument)
  - [timestamp_cmp_timestamptz_internal](timestamp_cmp_timestamptz_internal.md) (internal comparison function)
  - PG_RETURN_INT32 (macro to return 32-bit integer result)
- Called from:
  - No direct references found (likely called through PostgreSQL's operator framework)

## Notes and Other Information
- This function is part of PostgreSQL's timestamp comparison operator implementation
- Returns the negated result of the internal comparison function to maintain proper comparison semantics
- Used for sorting and ordering operations involving timestamptz and timestamp types
- The comparison handles timezone considerations through the internal function
- Located in src/backend/utils/adt/timestamp.c:2463-2482