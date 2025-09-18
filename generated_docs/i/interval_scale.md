# interval_scale

## Location
src/backend/utils/adt/timestamp.c: 1337 - 1358

## Overview
Adjusts an INTERVAL value to conform to specified type modifier constraints, performing truncation and precision adjustments as required by the target interval type specification.

## Definition
```c
Datum interval_scale(PG_FUNCTION_ARGS)
```

## Detailed Description
The `interval_scale` function is used by PostgreSQL's type system to convert interval values to match specific type modifier requirements. It creates a new interval that conforms to the field range and precision constraints specified in the typmod parameter.

This function serves as the runtime implementation for interval type conversions and is typically called when:
- Storing intervals in typed columns with specific constraints
- Converting between different interval type specifications
- Applying explicit casts with type modifier information

The function creates a copy of the input interval and delegates the actual adjustment logic to `AdjustIntervalForTypmod()`, which handles the complex truncation and precision adjustment operations.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:
  - `interval`: Input Interval pointer from PG_GETARG_INTERVAL_P(0)
  - `typmod`: 32-bit type modifier specifying target constraints from PG_GETARG_INT32(1)
  - `result`: Newly allocated Interval structure for the adjusted result

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INTERVAL_P
  - PG_GETARG_INT32
  - palloc
  - Interval (data structure)
  - AdjustIntervalForTypmod
  - PG_RETURN_INTERVAL_P
- Called from (representative examples):
  - PostgreSQL type system during interval type conversions
  - Column value storage and retrieval operations
  - Explicit interval type casts

## Notes and Other Information
- Creates a copy of the input interval rather than modifying it in place
- The actual adjustment logic is implemented in `AdjustIntervalForTypmod()`
- Used by PostgreSQL's type system infrastructure for "stuffing columns" with properly constrained values
- Part of the standard PostgreSQL function infrastructure for interval data type operations
- Companion to `interval_support()` which optimizes away unnecessary calls to this function
- Essential for maintaining data type integrity when storing intervals with specific type constraints