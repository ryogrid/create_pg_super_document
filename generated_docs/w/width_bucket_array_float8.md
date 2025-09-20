# width_bucket_array_float8

## Location
[src/backend/utils/adt/arrayfuncs.c:6741-6784](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L6741-L6784)

## Overview
A specialized implementation of width_bucket for float8 (double precision) data types, providing optimized performance for numeric threshold arrays.

## Definition

```c
static int
width_bucket_array_float8(Datum operand, ArrayType *thresholds)
```
## Detailed Description
This function implements a high-performance version of width bucketing specifically for float8 data types. It uses binary search to efficiently locate the appropriate bucket for the given operand value within the sorted thresholds array.

The function handles special cases for IEEE floating-point arithmetic, particularly NaN (Not-a-Number) values. NaN operands are considered greater than or equal to all threshold values, including other NaNs, and are assigned to the highest bucket number.

The binary search algorithm efficiently narrows down the search space by comparing the operand against the middle threshold value and adjusting the search bounds accordingly.

## Parameters / Member Variables
- : The float8 value to be bucketed (passed as Datum)
- : ArrayType containing sorted float8 threshold values with no NULLs

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetFloat8](../D/DatumGetFloat8.md)
  - ARR_DATA_PTR
  - ArrayGetNItems
  - ARR_NDIM
  - ARR_DIMS
  - isnan
- Called from:
  - [width_bucket_array](width_bucket_array.md) (src/backend/utils/adt/arrayfuncs.c:6699)

## Notes and Other Information
- Uses direct array indexing since NULL values are guaranteed to be absent
- Implements binary search for O(log n) time complexity
- Handles NaN values correctly by treating them as greater than all other values
- The search algorithm finds the rightmost position among equal threshold values
- Returns the bucket number (0 to N, where N is the number of thresholds)
- Static function, only accessible within the same compilation unit