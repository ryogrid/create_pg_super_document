# float4_le

## Location
src/include/utils/float.h: 298 - 303

## Overview
Compares two single-precision floating-point values to determine if the first value is less than or equal to the second, with proper NaN handling according to IEEE 754 standards.

## Definition


## Detailed Description
This inline function implements the "less than or equal to" comparison for single-precision floating-point numbers (float4). The function follows IEEE 754 semantics where any comparison involving NaN returns false, except for this specific case where NaN is considered "greater than" any other value. This means if val2 is NaN, the function returns true (since anything is ≤ NaN), and if val1 is NaN but val2 is not, it returns false.

The implementation uses short-circuit evaluation: if val2 is NaN, it immediately returns true; otherwise, it checks that val1 is not NaN and performs the standard floating-point comparison.

## Parameters / Member Variables
- : The first single-precision floating-point value (left operand of the ≤ comparison)
- : The second single-precision floating-point value (right operand of the ≤ comparison)

## Dependencies
- Functions called/Symbols referenced:
  - float4 (type definition for single-precision float)
  - isnan (standard library function for NaN detection)
- Called from (representative examples):
  - [float4le](float4le.md) (wrapper function in src/backend/utils/adt/float.c:851)

## Notes and Other Information
- This is an inline function defined in the header file for performance optimization
- Implements PostgreSQL's specific NaN handling semantics which may differ from standard C comparisons
- Part of PostgreSQL's comprehensive floating-point arithmetic infrastructure
- The NaN handling ensures consistent behavior across different platforms and compilers