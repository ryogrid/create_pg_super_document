# int8_accum_inv

## Location
src/backend/utils/adt/numeric.c: 6040 - 6060

## Overview
Inverse accumulation function for 64-bit integer aggregate operations that removes a value from the running aggregate state.

## Definition
```c
Datum int8_accum_inv(PG_FUNCTION_ARGS)
```

## Detailed Description
The `int8_accum_inv` function serves as the inverse accumulation function for aggregate operations involving 64-bit integers. It's designed for use in window functions and moving aggregates where values need to be removed from the running state as the window slides. The function takes a `NumericAggState` pointer and a 64-bit integer value to remove from the accumulation state.

Unlike its 32-bit counterpart, this function uses only numeric operations for all platforms, converting the 64-bit integer to numeric format before performing the discard operation. It includes proper validation to ensure the state is not NULL and handles error conditions appropriately.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `state`: NumericAggState pointer representing the current aggregate state
  - `value`: int64 value to be removed from the accumulation

## Dependencies
- Functions called/Symbols referenced:
  - [NumericAggState](../N/NumericAggState.md) (aggregate state structure)
  - [do_numeric_discard](../d/do_numeric_discard.md) (numeric discard operation)
  - [int64_to_numeric](int64_to_numeric.md) (conversion function)
  - `PG_GETARG_INT64` (argument extraction macro)
- Called from (representative examples):
  - No direct references found in the indexed codebase

## Notes and Other Information
- Uses `NumericAggState` instead of `PolyNumAggState` unlike the int4 variant
- Always uses numeric operations regardless of platform capabilities
- Part of PostgreSQL's aggregate function framework for window functions and moving aggregates
- Includes error handling for both NULL state and failed discard operations
- Located in src/backend/utils/adt/numeric.c:6040-6060