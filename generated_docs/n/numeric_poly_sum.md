# numeric_poly_sum

## Location
src/backend/utils/adt/numeric.c: 6086 - 6113

## Overview
Final function for polymorphic sum aggregate operations that computes the final sum result from the accumulated state.

## Definition
```c
Datum numeric_poly_sum(PG_FUNCTION_ARGS)
```

## Detailed Description
The `numeric_poly_sum` function serves as the final function for polymorphic sum aggregate operations. It takes the accumulated state and produces the final sum result. The function uses conditional compilation to optimize performance on platforms with 128-bit integer support, directly converting the 128-bit sum to a numeric result. On platforms without 128-bit support, it falls back to using the standard `numeric_sum` function.

The function handles the case where no non-null inputs were processed by returning NULL, following standard SQL aggregate behavior. When 128-bit arithmetic is available, it efficiently converts the accumulated `sumX` value from the state to a numeric result using specialized conversion functions.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `state`: PolyNumAggState pointer containing the accumulated sum and count

## Dependencies
- Functions called/Symbols referenced:
  - `PolyNumAggState` (polymorphic aggregate state structure)
  - `Numeric` (PostgreSQL numeric type)
  - `init_var` (variable initialization)
  - `[int128_to_numericvar](../i/int128_to_numericvar.md)` (128-bit to numeric conversion)
  - `[make_result](../m/make_result.md)` (result creation)
  - `[free_var](../f/free_var.md)` (variable cleanup)
  - `[numeric_sum](numeric_sum.md)` (fallback sum function)
  - `HAVE_INT128` (compilation flag)
- Called from (representative examples):
  - No direct references found in the indexed codebase

## Notes and Other Information
- Uses conditional compilation for optimal performance on 128-bit platforms
- Falls back to `numeric_sum` on platforms without 128-bit integer support
- Part of PostgreSQL's polymorphic aggregate function framework
- Handles NULL state appropriately by returning NULL for empty result sets
- Uses proper memory management with `init_var` and `free_var`
- Located in src/backend/utils/adt/numeric.c:6086-6113