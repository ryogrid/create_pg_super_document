# numeric_smaller

## Location
[src/backend/utils/adt/numeric.c:3486-3507](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L3486-L3507)

## Overview
Returns the smaller of two numeric values, handling NaN comparisons consistently with PostgreSQL comparison operators.

## Definition
```c
Datum numeric_smaller(PG_FUNCTION_ARGS)
```

## Detailed Description
The `numeric_smaller` function implements the SQL `LEAST()` functionality for numeric data types. It takes two numeric arguments and returns the smaller value. The function uses `cmp_numerics()` internally to ensure consistent comparison behavior, particularly for special cases involving NaN (Not a Number) values. This ensures that the result agrees with PostgreSQL comparison operators and maintains consistency across the numeric type system.

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
- The function prioritizes consistency with PostgreSQL comparison operators over simple numeric comparison
- NaN handling follows PostgreSQL standards where NaN comparisons have specific behavior
- Returns the first argument if both values are equal
- Part of the PostgreSQL numeric type system in src/backend/utils/adt/numeric.c

## Simplified Source

```c
Datum
numeric_smaller(PG_FUNCTION_ARGS)
{
    Numeric num1 = PG_GETARG_NUMERIC(0);
    Numeric num2 = PG_GETARG_NUMERIC(1);

    // Compare using standard numeric comparison (handles NaN correctly)
    if (cmp_numerics(num1, num2) < 0)
        PG_RETURN_NUMERIC(num1);  // num1 is smaller
    else
        PG_RETURN_NUMERIC(num2);  // num2 is smaller or equal
}
```