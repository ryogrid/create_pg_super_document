# array_cardinality

## Location
[src/backend/utils/adt/arrayfuncs.c:1790-1819](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L1790-L1819)

## Overview
Returns the total number of elements in an array, providing the cardinality (total count) of all elements across all dimensions.

## Definition
```c
Datum array_cardinality(PG_FUNCTION_ARGS)
```

## Detailed Description
The `array_cardinality` function calculates and returns the total number of elements contained within a PostgreSQL array. Unlike functions that return dimensional information, this function provides the absolute count of all elements regardless of the array's dimensional structure. It works with any array type through the `AnyArrayType` interface and uses the internal `ArrayGetNItems` function to compute the total element count based on the array's dimensions.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Array input (accessed via `PG_GETARG_ANY_ARRAY_P(0)`): The array whose cardinality is to be calculated

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_ANY_ARRAY_P`: Retrieves the array argument
  - `[ArrayGetNItems](../A/ArrayGetNItems.md)`: Calculates total number of items from dimensions
  - `AARR_NDIM`: Gets the number of dimensions of the array
  - `AARR_DIMS`: Gets the dimension sizes array
- Called from (representative examples):
  - SQL queries using the `cardinality()` function
  - Array processing functions requiring element counts

## Notes and Other Information
- Returns an INT32 value representing the total element count
- Works with multi-dimensional arrays by multiplying all dimension sizes
- Part of PostgreSQL's array utility functions in `src/backend/utils/adt/arrayfuncs.c`
- Provides O(1) complexity as it uses pre-calculated dimension information
- Handles empty arrays by returning 0

## Simplified Source

```c
Datum
array_cardinality(PG_FUNCTION_ARGS)
{
    AnyArrayType *v = PG_GETARG_ANY_ARRAY_P(0);

    // Calculate total number of elements across all dimensions
    PG_RETURN_INT32(ArrayGetNItems(AARR_NDIM(v), AARR_DIMS(v)));
}
```