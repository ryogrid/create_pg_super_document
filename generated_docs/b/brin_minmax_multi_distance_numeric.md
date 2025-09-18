# brin_minmax_multi_distance_numeric

## Location
src/backend/access/brin/brin_minmax_multi.c: 2021 - 2046

## Overview
Computes the distance between two numeric values using plain subtraction for BRIN minmax multi indexes.

## Definition
```c
Datum brin_minmax_multi_distance_numeric(PG_FUNCTION_ARGS)
```

## Detailed Description
This function calculates the distance between two numeric range boundaries in BRIN minmax multi indexes. It performs a simple subtraction operation (a2 - a1) where the arguments are expected to be ordered such that a1 ≤ a2. The function is used internally by the BRIN minmax multi operator class to determine the distance metric for numeric data types, which helps in index optimization and range query processing.

The function includes an assertion to verify that the first argument is less than or equal to the second argument, ensuring the range boundaries are properly ordered. The result is converted to a float8 value for consistent distance representation across different data types.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0 (a1): First numeric value (lower bound)
  - Argument 1 (a2): Second numeric value (upper bound)

## Dependencies
- Functions called/Symbols referenced:
  - `DirectFunctionCall2`: PostgreSQL direct function call mechanism
  - `DirectFunctionCall1`: PostgreSQL direct function call mechanism  
  - `numeric_le`: Numeric less-than-or-equal comparison function
  - `numeric_sub`: Numeric subtraction function
  - `numeric_float8`: Numeric to float8 conversion function
  - `PG_RETURN_DATUM`: PostgreSQL return value macro
- Called from (representative examples):
  - No direct references found (likely referenced through function pointers in BRIN operator classes)

## Notes and Other Information
- The function assumes that range boundaries may be collapsed (single points with equal values)
- Uses assertion checking to validate input ordering in debug builds
- Returns the distance as a float8 datum for consistency with other distance functions
- Part of the BRIN minmax multi access method implementation
- Located in src/backend/access/brin/brin_minmax_multi.c:2021-2046