# numeric_ne

## Location
[src/backend/utils/adt/numeric.c:2446-2460](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L2446-L2460)

## Overview
PostgreSQL built-in function that tests inequality between two numeric values, returning a boolean result.

## Definition
```c
Datum numeric_ne(PG_FUNCTION_ARGS)
```

## Detailed Description
The `numeric_ne` function is a PostgreSQL built-in function that implements inequality comparison for the numeric data type. It takes two numeric arguments and returns a boolean value indicating whether they are not equal. The function leverages the existing `cmp_numerics` comparison function and tests if the result is non-zero, which indicates inequality.

This function is part of PostgreSQL's operator framework and is typically invoked through the `<>` or `!=` operator for numeric values. It properly handles PostgreSQL's function calling conventions and memory management, ensuring that any copied numeric values are freed appropriately.

The inequality test is performed by calling the comprehensive `cmp_numerics` function and checking if the comparison result is not equal to zero, making this implementation both reliable and consistent with other numeric comparison operations.

## Parameters / Member Variables
- Argument 0: First numeric value to compare (accessed via PG_GETARG_NUMERIC(0))
- Argument 1: Second numeric value to compare (accessed via PG_GETARG_NUMERIC(1))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NUMERIC (extracts Numeric arguments from function call)
  - cmp_numerics (performs the actual numeric comparison)
  - PG_FREE_IF_COPY (frees copied numeric values if necessary)
  - PG_RETURN_BOOL (returns boolean result)
  - Numeric (PostgreSQL numeric data type)
- Called from (representative examples):
  - No direct references found in the current codebase analysis

## Notes and Other Information
- Implements the `<>` and `!=` operators for PostgreSQL numeric data type
- Returns true if numeric values are not equal, false if they are equal
- Properly handles all numeric representations including NaN, infinity, and various scales
- Uses the same comparison logic as other numeric comparison functions for consistency
- Follows PostgreSQL's V1 calling convention and memory management practices
- Can be called directly from SQL expressions or from internal C code
- Part of the complete set of numeric comparison operators in PostgreSQL
- Complementary function to `numeric_eq` with opposite boolean result logic