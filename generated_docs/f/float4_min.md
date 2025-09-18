# float4_min

## Location
src/include/utils/float.h: 334 - 339

## Overview
Returns the smaller of two single-precision floating-point numbers, with PostgreSQL's NaN handling semantics applied.

## Definition


## Detailed Description
This inline function implements the minimum operation for single-precision floating-point numbers (float4). It uses the float4_lt comparison function to determine which value is smaller and returns that value. The function inherits PostgreSQL's NaN handling behavior from float4_lt, where NaN comparisons follow specific SQL standard semantics.

The function performs a simple conditional selection: if val1 is less than val2 according to float4_lt, it returns val1; otherwise, it returns val2.

## Parameters / Member Variables
- : The first single-precision floating-point value to compare
- : The second single-precision floating-point value to compare

## Dependencies
- Functions called/Symbols referenced:
  - float4_lt (for less-than comparison with NaN handling)
  - float4 (single-precision floating-point type)
- Called from (representative examples):
  - Currently no direct references found in the codebase

## Notes and Other Information
- This is an inline function defined in the header for performance optimization
- Relies on float4_lt for the actual comparison logic and NaN handling
- Part of the float4 family of utility functions for single-precision arithmetic
- May be used internally by other mathematical or statistical functions
- The NaN behavior is consistent with PostgreSQL's floating-point semantics