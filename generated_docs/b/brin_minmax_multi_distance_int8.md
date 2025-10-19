# brin_minmax_multi_distance_int8

## Location
[src/backend/access/brin/brin_minmax_multi.c:1971-1989](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax_multi.c#L1971-L1989)

## Overview
Computes the distance between two int8 (64-bit integer) values used as range boundaries in BRIN minmax-multi indexes.

## Definition
```c
Datum brin_minmax_multi_distance_int8(PG_FUNCTION_ARGS)
```

## Detailed Description
This function calculates the distance between two 64-bit signed integer values for BRIN (Block Range Index) minmax-multi operator class. It performs plain subtraction to determine the range size, converting the integer values to double precision floating-point to avoid potential overflow issues and to maintain consistency with the distance function interface.

The function is designed specifically for range boundaries where the first argument should be less than or equal to the second argument. The result is returned as a float8 (double) value to ensure sufficient precision for distance calculations used in BRIN index operations.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function arguments containing:
  - Argument 0: First int8 value (range minimum)
  - Argument 1: Second int8 value (range maximum)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INT64`: Extracts int64 arguments from function call
  - `PG_RETURN_FLOAT8`: Returns float8 result from PostgreSQL function (implicitly referenced)
- Called from (representative examples):
  - No direct references found in codebase

## Notes and Other Information
- The function assumes that a1 <= a2 (enforced by Assert)
- [Integer](../I/Integer.md) values are cast to double to prevent overflow and maintain precision
- Returns distance as float8 for compatibility with BRIN distance function interface
- Used internally by BRIN minmax-multi operator class for int8 data types
- Part of the extensible operator class framework for BRIN indexes
- The distance calculation is essential for determining when ranges should be merged or split in multi-range BRIN summaries
- Note: For very large int8 values, some precision may be lost during the cast to double

## Simplified Source

```c
Datum brin_minmax_multi_distance_int8(PG_FUNCTION_ARGS) {
    // Extract 64-bit integer arguments
    int64 a1 = PG_GETARG_INT64(0);
    int64 a2 = PG_GETARG_INT64(1);

    // Calculate distance (a2 - a1) and convert to double
    PG_RETURN_FLOAT8((double) a2 - (double) a1);
}
```