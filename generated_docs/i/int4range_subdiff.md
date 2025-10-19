# int4range_subdiff

## Location
[src/backend/utils/adt/rangetypes.c:1621-1629](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L1621-L1629)

## Overview
Computes the difference between two 32-bit integer values, used as a subtype difference function for int4range operations.

## Definition

```c
Datum
int4range_subdiff(PG_FUNCTION_ARGS)
```
## Detailed Description
This function calculates the arithmetic difference between two int32 values and returns the result as a float8 (double precision) value. It is designed to be used as a subtype difference function for int4range types, which is essential for range operations that need to measure the "distance" or "size" of ranges. The function converts both integer inputs to float8 before performing the subtraction to avoid potential integer overflow issues.

The function returns the actual difference (v1 - v2), not the absolute value, so the result can be negative if v2 is greater than v1. This signed result is important for proper range calculations and comparisons.

## Parameters / Member Variables
- : The first 32-bit integer value (accessed via )
- : The second 32-bit integer value (accessed via )

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32 (used twice)
  - PG_RETURN_FLOAT8
- Called from (representative examples):
  - No direct references found (likely called via function catalog for range operations)

## Notes and Other Information
- Part of the subtype difference functions for built-in range types
- Returns the signed difference, not the absolute value
- Uses float8 arithmetic to avoid integer overflow
- The comment notes that subtype_diff functions must take care to avoid overflow
- Essential for range operations that require measuring distances between range bounds
- Simple implementation that handles the int32 to float8 conversion automatically

## Simplified Source

```c
Datum int4range_subdiff(PG_FUNCTION_ARGS) {
    // Extract the two integer arguments
    int32 v1 = PG_GETARG_INT32(0);
    int32 v2 = PG_GETARG_INT32(1);

    // Return the difference as float8 to avoid overflow
    PG_RETURN_FLOAT8((float8) v1 - (float8) v2);
}
```