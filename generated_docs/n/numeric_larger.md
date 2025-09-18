# numeric_larger

## Location
src/backend/utils/adt/numeric.c: 3508 - 3536

## Overview
Returns the larger of two numeric values, handling NaN comparisons consistently with PostgreSQL comparison operators.

## Definition
```c
Datum numeric_larger(PG_FUNCTION_ARGS)
```

## Detailed Description
The `numeric_larger` function implements the SQL `GREATEST()` functionality for numeric data types. It takes two numeric arguments and returns the larger value. Similar to `numeric_smaller`, it uses `cmp_numerics()` internally to ensure consistent comparison behavior, particularly for special cases involving NaN (Not a Number) values. This ensures that the result agrees with PostgreSQL comparison operators and maintains consistency across the numeric type system.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `num1` (Numeric): First numeric value to compare
  - `num2` (Numeric): Second numeric value to compare

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_NUMERIC`: Extracts numeric arguments from function call context
  - [cmp_numerics](../c/cmp_numerics.md): Performs numeric comparison with proper NaN handling
  - `PG_RETURN_NUMERIC`: Returns numeric result to caller
- Called from (representative examples):
  - No direct references found (likely called via SQL function dispatch)

## Notes and Other Information
- Counterpart to `numeric_smaller`, implementing the opposite comparison logic
- The function prioritizes consistency with PostgreSQL comparison operators over simple numeric comparison
- NaN handling follows PostgreSQL standards where NaN comparisons have specific behavior
- Returns the second argument if both values are equal
- Part of the PostgreSQL numeric type system in src/backend/utils/adt/numeric.c