# numeric_ge

## Location
src/backend/utils/adt/numeric.c: 2476 - 2490

## Overview
PostgreSQL function that compares two numeric values and returns true if the first value is greater than or equal to the second.

## Definition
```c
Datum numeric_ge(PG_FUNCTION_ARGS)
```

## Detailed Description
The `numeric_ge` function implements the greater-than-or-equal-to comparison operator (>=) for PostgreSQL's NUMERIC data type. This function is part of the comprehensive set of numeric comparison operators and serves as the backend implementation for SQL expressions like `SELECT 5.5 >= 3.2` or `SELECT 3.2 >= 3.2`.

The function extracts two NUMERIC arguments from the function call arguments, delegates the actual comparison logic to the `cmp_numerics` helper function, and returns a boolean result indicating whether the first numeric value is greater than or equal to the second. The comparison succeeds when `cmp_numerics` returns 0 (equal) or a positive value (greater than).

## Parameters / Member Variables
- Function arguments accessed via `PG_FUNCTION_ARGS` macro:
  - First argument (index 0): First NUMERIC value for comparison
  - Second argument (index 1): Second NUMERIC value for comparison

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_NUMERIC` (macro to extract NUMERIC arguments)
  - `cmp_numerics` (core comparison logic function)
  - `PG_FREE_IF_COPY` (memory management macro)
  - `PG_RETURN_BOOL` (macro to return boolean result)
- Called from:
  - SQL greater-than-or-equal-to operator expressions
  - PostgreSQL operator dispatch system
  - Numeric comparison operations

## Notes and Other Information
- The function follows PostgreSQL's standard function calling convention using `PG_FUNCTION_ARGS`
- Memory management is handled through `PG_FREE_IF_COPY` to ensure proper cleanup of potentially large numeric values
- The actual comparison logic is centralized in `cmp_numerics`, which handles special cases like NaN and infinity values
- Part of the complete set of numeric comparison operators (=, <>, <, <=, >, >=)
- Located in `src/backend/utils/adt/numeric.c:2476-2490`
- Uses `>= 0` comparison on the result of `cmp_numerics` to implement the >= logic