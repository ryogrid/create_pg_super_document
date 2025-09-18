# numeric_poly_avg

## Location
src/backend/utils/adt/numeric.c: 6114 - 6143

## Overview
Final function for polymorphic average aggregate operations that computes the final average result by dividing the accumulated sum by the count.

## Definition
```c
Datum numeric_poly_avg(PG_FUNCTION_ARGS)
```

## Detailed Description
The `numeric_poly_avg` function serves as the final function for polymorphic average aggregate operations. It takes the accumulated state containing both sum and count, and produces the final average result by performing division. The function uses conditional compilation to optimize performance on platforms with 128-bit integer support, directly converting the accumulated values and performing the division using PostgreSQL's numeric division function.

On platforms without 128-bit support, it falls back to using the standard `numeric_avg` function. When 128-bit arithmetic is available, it converts both the accumulated sum and count to numeric values, then uses `DirectFunctionCall2` to perform the division operation through `numeric_div`.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `state`: PolyNumAggState pointer containing the accumulated sum (sumX) and count (N)

## Dependencies
- Functions called/Symbols referenced:
  - `PolyNumAggState` (polymorphic aggregate state structure)
  - `init_var` (variable initialization)
  - `int128_to_numericvar` (128-bit to numeric conversion)
  - `int64_to_numeric` (int64 to numeric conversion)
  - `NumericGetDatum` (numeric to datum conversion)
  - `make_result` (result creation)
  - `free_var` (variable cleanup)
  - `DirectFunctionCall2` (direct function call mechanism)
  - `numeric_div` (numeric division function)
  - `numeric_avg` (fallback average function)
  - `HAVE_INT128` (compilation flag)
- Called from (representative examples):
  - No direct references found in the indexed codebase

## Notes and Other Information
- Uses conditional compilation for optimal performance on 128-bit platforms
- Falls back to `numeric_avg` on platforms without 128-bit integer support
- Performs division using PostgreSQL's built-in numeric division function
- Part of PostgreSQL's polymorphic aggregate function framework
- Handles NULL state appropriately by returning NULL for empty result sets
- Uses proper memory management and datum conversion functions
- Located in src/backend/utils/adt/numeric.c:6114-6143