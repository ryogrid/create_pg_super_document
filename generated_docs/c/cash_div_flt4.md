# cash_div_flt4

## Location
[src/backend/utils/adt/cash.c:800-812](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/cash.c#L800-L812)

## Overview
A PostgreSQL function that performs division of a Cash value by a float4 value, returning the result as a Cash type.

## Definition
```c
Datum cash_div_flt4(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the division operation between a PostgreSQL Cash value and a single-precision floating-point number (float4). It serves as a wrapper function that extracts the arguments, converts the float4 divisor to float8 for precision consistency, and delegates the actual computation to the `cash_div_float8` helper function. This ensures uniform handling of floating-point arithmetic with Cash values and maintains precision across different floating-point types.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: Cash value (dividend) to be divided
  - Argument 1: float4 value (divisor) to divide by

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CASH: Extracts Cash argument from function call context
  - PG_GETARG_FLOAT4: Extracts float4 argument from function call context
  - [cash_div_float8](cash_div_float8.md): Performs the actual division with float8 precision
  - PG_RETURN_CASH: Returns the computed Cash result
- Called from:
  - SQL operator implementations for money / float4 operations

## Notes and Other Information
- The function promotes the float4 divisor to float8 before computation to maintain precision consistency
- Located in src/backend/utils/adt/cash.c:800-812
- Part of PostgreSQL's monetary data type arithmetic operations
- Division by zero and other edge cases are handled by the underlying cash_div_float8 implementation
- Maintains the same error handling and overflow protection as other Cash division operations

## Simplified Source

```c
Datum cash_div_flt4(PG_FUNCTION_ARGS) {
    // Extract cash value and float4 divisor
    Cash c = PG_GETARG_CASH(0);
    float4 f = PG_GETARG_FLOAT4(1);

    // Convert float4 to float8 and delegate to helper function
    PG_RETURN_CASH(cash_div_float8(c, (float8) f));
}
```