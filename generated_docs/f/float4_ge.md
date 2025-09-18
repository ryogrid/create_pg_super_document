# float4_ge

## Location
src/include/utils/float.h: 322 - 327

## Overview
Compares two single-precision floating-point values to determine if the first value is greater than or equal to the second, with proper NaN handling according to IEEE 754 standards.

## Definition
```c
static inline bool
float4_ge(const float4 val1, const float4 val2)
```

## Detailed Description
This inline function implements the "greater than or equal to" comparison for single-precision floating-point numbers (float4). Following PostgreSQL's IEEE 754 NaN semantics, the function treats NaN as the largest possible value. If val1 is NaN, the function immediately returns true (NaN >= anything). Otherwise, it ensures val2 is not NaN before performing the standard floating-point comparison.

The implementation uses short-circuit evaluation: first check if val1 is NaN (immediate true), then verify that val2 is not NaN and perform the numerical comparison val1 >= val2.

## Parameters / Member Variables
- `val1`: The first single-precision floating-point value (left operand of the >= comparison)
- `val2`: The second single-precision floating-point value (right operand of the >= comparison)

## Dependencies
- Functions called/Symbols referenced:
  - float4 (type definition for single-precision float)
  - isnan (standard library function for NaN detection)
- Called from (representative examples):
  - [float4ge](float4ge.md) (wrapper function in src/backend/utils/adt/float.c:869)

## Notes and Other Information
- This is an inline function defined in the header file for performance optimization
- Implements PostgreSQL's consistent NaN handling where NaN is considered greater than any other value
- Part of the complete set of floating-point comparison functions in PostgreSQL
- Less frequently used than other comparison operators but essential for completeness
- Follows the same logical pattern as other PostgreSQL floating-point comparison functions
- Used primarily through the wrapper function float4ge in SQL contexts