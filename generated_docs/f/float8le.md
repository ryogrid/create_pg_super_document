# float8le

## Location
src/backend/utils/adt/float.c: 940 - 948

## Overview
The float8le function implements the less-than-or-equal-to comparison operator for PostgreSQL double-precision floating-point numbers, with proper handling of NaN values according to IEEE 754 standards.

## Definition
```c
Datum float8le(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL SQL-callable wrapper that performs less-than-or-equal-to comparison between two float8 (double-precision) values. It extracts two float8 arguments from the function call context and delegates the actual comparison logic to the inline helper function `float8_le`. The function implements IEEE 754-compliant NaN handling where NaN is considered greater than any finite value, so any finite value is less than or equal to NaN.

The underlying comparison logic in `float8_le` implements the rule that:
- If the second value is NaN, the result is always true (any value <= NaN)
- If the second value is not NaN but the first value is NaN, the result is false
- Otherwise, it performs a standard floating-point less-than-or-equal comparison

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: PostgreSQL function call context containing:
  - First argument (index 0): float8 value (left operand)
  - Second argument (index 1): float8 value (right operand)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8 (macro to extract float8 arguments)
  - [float8_le](float8_le.md) (inline helper function for the actual comparison)
  - PG_RETURN_BOOL (macro to return boolean result)
- Called from (representative examples):
  - SQL queries using the <= operator with float8 operands

## Notes and Other Information
- Located in src/backend/utils/adt/float.c:940-948
- This function is typically invoked through PostgreSQLs operator system rather than direct calls
- The NaN handling follows the convention that NaN is treated as greater than any finite value
- Part of PostgreSQLs comprehensive floating-point arithmetic system for ordered comparisons