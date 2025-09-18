# brin_minmax_multi_distance_timetz

## Location
[src/backend/access/brin/brin_minmax_multi.c:2119-2136](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax_multi.c#L2119-L2136)

## Overview
Computes the distance between two time with timezone values for BRIN minmax multi indexes by combining time and timezone offset differences.

## Definition
```c
Datum brin_minmax_multi_distance_timetz(PG_FUNCTION_ARGS)
```

## Detailed Description
This function calculates the distance between two time with timezone range boundaries in BRIN minmax multi indexes. It handles both the time component and the timezone offset component of TimeTzADT values.

The calculation involves subtracting the time components (stored as int64 microseconds since midnight) and adjusting for timezone differences. The timezone offset difference is converted to microseconds by multiplying by USECS_PER_SEC and added to the time difference. This produces a comprehensive distance measurement that accounts for both temporal and geographic (timezone) differences.

The function is used internally by the BRIN minmax multi operator class for time with timezone data types to support index optimization and range query processing.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0 (ta): First time with timezone value (lower bound) as TimeTzADT pointer
  - Argument 1 (tb): Second time with timezone value (upper bound) as TimeTzADT pointer

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_TIMETZADT_P`: PostgreSQL macro to extract TimeTzADT pointer from function arguments
  - `TimeTzADT`: PostgreSQL time with timezone abstract data type structure
  - `USECS_PER_SEC`: Constant for microseconds per second conversion
  - `PG_RETURN_FLOAT8`: PostgreSQL return float8 value macro
  - `zone`: Member of TimeTzADT structure representing timezone offset
  - `time`: Member of TimeTzADT structure representing time component
- Called from (representative examples):
  - No direct references found (likely referenced through function pointers in BRIN operator classes)

## Notes and Other Information
- Handles both time and timezone components in the distance calculation
- TimeTzADT contains separate fields for time (microseconds since midnight) and zone (seconds offset from UTC)
- Timezone offset is converted from seconds to microseconds for consistent units
- Includes assertion checking to validate non-negative result
- Returns combined distance as a float8 value for consistency across BRIN distance functions
- More complex than regular time distance due to timezone considerations
- Part of the BRIN minmax multi access method implementation
- Located in src/backend/access/brin/brin_minmax_multi.c:2119-2136