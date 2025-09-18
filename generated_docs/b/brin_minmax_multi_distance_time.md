# brin_minmax_multi_distance_time

## Location
[src/backend/access/brin/brin_minmax_multi.c:2099-2118](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax_multi.c#L2099-L2118)

## Overview
Computes the distance between two time (without timezone) values for BRIN minmax multi indexes using direct subtraction.

## Definition
```c
Datum brin_minmax_multi_distance_time(PG_FUNCTION_ARGS)
```

## Detailed Description
This function calculates the distance between two time range boundaries in BRIN minmax multi indexes. Since TimeADT is internally represented as a 64-bit integer (int64) storing microseconds since midnight, the function performs direct subtraction between the two values to compute the time difference.

The result represents the time difference in microseconds between the two time values, which is then returned as a float8 value for consistency with other BRIN distance functions. This function is used internally by the BRIN minmax multi operator class for time data types to support index optimization and range query processing.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0 (ta): First time value (lower bound) as TimeADT
  - Argument 1 (tb): Second time value (upper bound) as TimeADT

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_TIMEADT`: PostgreSQL macro to extract TimeADT from function arguments
  - `TimeADT`: PostgreSQL time abstract data type (int64 representing microseconds since midnight)
  - `PG_RETURN_FLOAT8`: PostgreSQL return float8 value macro
- Called from (representative examples):
  - No direct references found (likely referenced through function pointers in BRIN operator classes)

## Notes and Other Information
- TimeADT is stored as int64 representing microseconds since midnight
- Uses direct integer subtraction for precise time difference calculation
- Includes assertion checking to validate non-negative result
- Returns distance in microseconds as a float8 value for consistency across BRIN distance functions
- Simpler implementation compared to timezone-aware time functions
- Part of the BRIN minmax multi access method implementation
- Located in src/backend/access/brin/brin_minmax_multi.c:2099-2118