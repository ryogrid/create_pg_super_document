# float4_max

## Location
[src/include/utils/float.h:346-351](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/float.h#L346-L351)

## Overview
Returns the larger of two single-precision floating-point numbers, with PostgreSQL's NaN handling semantics applied.

## Definition

```c
static inline float4
float4_max(const float4 val1, const float4 val2)
```
## Detailed Description
This inline function implements the maximum operation for single-precision floating-point numbers (float4). It uses the float4_gt comparison function to determine which value is larger and returns that value. The function inherits PostgreSQL's NaN handling behavior from float4_gt, where NaN comparisons follow specific SQL standard semantics.

The function performs a simple conditional selection: if val1 is greater than val2 according to float4_gt, it returns val1; otherwise, it returns val2.

## Parameters / Member Variables
- : The first single-precision floating-point value to compare
- : The second single-precision floating-point value to compare

## Dependencies
- Functions called/Symbols referenced:
  - [float4_gt](float4_gt.md) (for greater-than comparison with NaN handling)
  - float4 (single-precision floating-point type)
- Called from (representative examples):
  - Currently no direct references found in the codebase

## Notes and Other Information
- This is an inline function defined in the header for performance optimization
- Relies on float4_gt for the actual comparison logic and NaN handling
- Part of the float4 family of utility functions for single-precision arithmetic
- May be used internally by other mathematical or statistical functions
- The NaN behavior is consistent with PostgreSQL's floating-point semantics
- Complement to the float4_min function for maximum value selection