# float4_gt

## Location
src/include/utils/float.h: 310 - 315

## Overview
Compares two single-precision floating-point values to determine if the first value is greater than the second, with proper NaN handling according to IEEE 754 standards.

## Definition
```c
static inline bool
float4_gt(const float4 val1, const float4 val2)
```

## Detailed Description
This inline function implements the "greater than" comparison for single-precision floating-point numbers (float4). The function follows IEEE 754 semantics with a specific interpretation where NaN is considered "greater than" any non-NaN value. The logic is inverted compared to the "less than or equal" functions: if val2 is NaN, it returns false (since nothing can be greater than NaN in this context), but if val1 is NaN and val2 is not, it returns true (NaN > non-NaN).

The implementation uses short-circuit evaluation: first check that val2 is not NaN, then check if val1 is NaN OR if val1 is numerically greater than val2.

## Parameters / Member Variables
- `val1`: The first single-precision floating-point value (left operand of the > comparison)
- `val2`: The second single-precision floating-point value (right operand of the > comparison)

## Dependencies
- Functions called/Symbols referenced:
  - float4 (type definition for single-precision float)
  - isnan (standard library function for NaN detection)
- Called from (representative examples):
  - float4larger (max function implementation)
  - float4_cmp_internal (internal comparison function)
  - float4gt (wrapper function in src/backend/utils/adt/float.c:860)
  - float4_max (maximum value computation macro)

## Notes and Other Information
- This is an inline function defined in the header file for performance optimization
- Used extensively in aggregate functions (MAX), comparison operations, and sorting
- Implements PostgreSQL's specific NaN ordering semantics which treat NaN as the largest value
- The NaN handling is consistent with PostgreSQL's overall approach to floating-point comparisons
- Critical for proper functioning of indexes, sorting, and aggregate operations involving float4 data