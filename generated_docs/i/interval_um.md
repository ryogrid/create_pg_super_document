# interval_um

## Location
src/backend/utils/adt/timestamp.c: 3405 - 3417

## Overview
The `interval_um` function implements the unary minus operator for PostgreSQL interval data types, negating all components of an interval value.

## Definition
```c
Datum interval_um(PG_FUNCTION_ARGS)
```

## Detailed Description
The `interval_um` function serves as the PostgreSQL SQL function interface for negating interval values. It acts as a wrapper around the internal `interval_um_internal` function, handling memory allocation and PostgreSQL function calling conventions. When applied to an interval, it negates all three components (time, day, month) while properly handling special cases like infinite intervals and overflow conditions.

The function follows PostgreSQL's standard function interface pattern, extracting arguments using `PG_GETARG_INTERVAL_P`, allocating memory for the result, and returning the result using `PG_RETURN_INTERVAL_P`.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `interval`: Input interval value to be negated

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INTERVAL_P` - Extract interval argument from function call
  - `palloc` - Allocate memory for result interval
  - `interval_um_internal` - Perform the actual interval negation logic
  - `PG_RETURN_INTERVAL_P` - Return the negated interval result
- Called from (representative examples):
  - No direct callers found (likely called through PostgreSQL SQL function dispatch)

## Notes and Other Information
- The actual negation logic is implemented in `interval_um_internal` located at src/backend/utils/adt/timestamp.c:3381-3402
- Handles special infinite interval cases (NOBEGIN/NOEND) by swapping them appropriately
- Includes overflow protection when negating interval components
- Raises ERROR with ERRCODE_DATETIME_VALUE_OUT_OF_RANGE if overflow occurs
- Part of PostgreSQL's interval arithmetic operations exposed as SQL functions