# cash_mul_int2

## Location
[src/backend/utils/adt/cash.c:891-902](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/cash.c#L891-L902)

## Overview
A PostgreSQL function that multiplies a Cash value by a 16-bit signed integer, providing safe arithmetic operations for monetary calculations with small integer values.

## Definition

```c
Datum
cash_mul_int2(PG_FUNCTION_ARGS)
```
## Detailed Description
The `cash_mul_int2` function is a PostgreSQL built-in function that performs multiplication of a Cash data type by a 16-bit signed integer (int2). It serves as a wrapper around the internal `cash_mul_int64` helper function, promoting the int2 parameter to int64 for consistent internal arithmetic handling. This function extends PostgreSQL's monetary arithmetic capabilities to work with smaller integer types, ensuring safe multiplication operations without integer overflow issues.

Like other Cash arithmetic functions, it follows PostgreSQL's standard function calling convention and provides type-safe monetary calculations.

## Parameters / Member Variables
- `c` (Cash): The monetary value to be multiplied (first argument)
- `s` (int16): The 16-bit signed integer multiplier (second argument)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_CASH`: Extracts Cash value from function arguments
  - `PG_GETARG_INT16`: Extracts int16 value from function arguments
  - [cash_mul_int64](cash_mul_int64.md): Internal helper function for safe Cash multiplication
  - `PG_RETURN_CASH`: Returns Cash value as function result
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL operator interface)

## Notes and Other Information
- This function delegates to the same `cash_mul_int64` helper used by other Cash multiplication functions, ensuring consistent behavior across different integer sizes
- Part of PostgreSQL's comprehensive monetary data type system supporting various integer sizes (int2, int4, int8)
- The int16 parameter is promoted to int64 internally for unified arithmetic handling
- Follows the same safe arithmetic pattern as `cash_mul_int4` but works with smaller 16-bit integers
- Provides support for multiplication operations where the multiplier fits in a smaller integer range

## Simplified Source

```c
Datum cash_mul_int2(PG_FUNCTION_ARGS) {
    // Extract cash value and int2 multiplier from arguments
    Cash c = PG_GETARG_CASH(0);
    int16 s = PG_GETARG_INT16(1);

    // Delegate to 64-bit multiplication helper for safe arithmetic
    PG_RETURN_CASH(cash_mul_int64(c, (int64) s));
}
```