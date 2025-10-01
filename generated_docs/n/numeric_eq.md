# numeric_eq

## Location
[src/backend/utils/adt/numeric.c:2431-2445](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L2431-L2445)

## Overview
PostgreSQL built-in function that tests equality between two numeric values, returning a boolean result.

## Definition
```c
Datum numeric_eq(PG_FUNCTION_ARGS)
```

## Detailed Description
The `numeric_eq` function is a PostgreSQL built-in function that implements equality comparison for the numeric data type. It takes two numeric arguments and returns a boolean value indicating whether they are equal. The function leverages the existing `cmp_numerics` comparison function and tests if the result is zero, which indicates equality.

This function is part of PostgreSQL's operator framework and is typically invoked through the `=` operator for numeric values. It properly handles PostgreSQL's function calling conventions and memory management, ensuring that any copied numeric values are freed appropriately.

The equality test is performed by calling the comprehensive `cmp_numerics` function and checking if the comparison result equals zero, making this implementation both reliable and consistent with other numeric comparison operations.

## Parameters / Member Variables
- Argument 0: First numeric value to compare (accessed via PG_GETARG_NUMERIC(0))
- Argument 1: Second numeric value to compare (accessed via PG_GETARG_NUMERIC(1))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NUMERIC (extracts Numeric arguments from function call)
  - [cmp_numerics](../c/cmp_numerics.md) (performs the actual numeric comparison)
  - PG_FREE_IF_COPY (frees copied numeric values if necessary)
  - PG_RETURN_BOOL (returns boolean result)
  - [Numeric](../N/Numeric.md) (PostgreSQL numeric data type)
- Called from (representative examples):
  - [equalsJsonbScalarValue](../e/equalsJsonbScalarValue.md) (JSON-B scalar value equality testing)

## Notes and Other Information
- Implements the `=` operator for PostgreSQL numeric data type
- Returns true if numeric values are exactly equal, false otherwise
- Properly handles all numeric representations including NaN, infinity, and various scales
- Uses the same comparison logic as other numeric comparison functions for consistency
- Follows PostgreSQL's V1 calling convention and memory management practices
- Can be called directly from SQL expressions or from internal C code
- Part of the complete set of numeric comparison operators in PostgreSQL

## Simplified Source

```c
Datum numeric_eq(PG_FUNCTION_ARGS) {
    Numeric num1 = PG_GETARG_NUMERIC(0);
    Numeric num2 = PG_GETARG_NUMERIC(1);

    // Compare numerics and check for equality (result == 0)
    bool result = cmp_numerics(num1, num2) == 0;

    // Clean up copied values
    PG_FREE_IF_COPY(num1, 0);
    PG_FREE_IF_COPY(num2, 1);

    PG_RETURN_BOOL(result);
}
```