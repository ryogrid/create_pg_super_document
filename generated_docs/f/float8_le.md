# float8_le

## Location
src/include/utils/float.h: 304 - 309

## Overview
Compares two double-precision floating-point values to determine if the first value is less than or equal to the second, with proper NaN handling according to IEEE 754 standards.

## Definition
```c
static inline bool
float8_le(const float8 val1, const float8 val2)
```

## Detailed Description
This inline function implements the "less than or equal to" comparison for double-precision floating-point numbers (float8). Like its single-precision counterpart float4_le, it follows IEEE 754 semantics for NaN handling where any comparison involving NaN returns false, except that NaN is considered "greater than" any other value. If val2 is NaN, the function returns true; if val1 is NaN but val2 is not, it returns false.

The implementation uses the same short-circuit evaluation pattern as float4_le: check if val2 is NaN first (immediate true), then verify val1 is not NaN before performing the standard floating-point comparison.

## Parameters / Member Variables
- `val1`: The first double-precision floating-point value (left operand of the ≤ comparison)
- `val2`: The second double-precision floating-point value (right operand of the ≤ comparison)

## Dependencies
- Functions called/Symbols referenced:
  - float8 (type definition for double-precision float)
  - isnan (standard library function for NaN detection)
- Called from (representative examples):
  - [size_box](../s/size_box.md) (geometric operations in GiST indexing)
  - [gist_box_picksplit](../g/gist_box_picksplit.md) (GiST index splitting algorithm)
  - [float8le](float8le.md) (wrapper function in src/backend/utils/adt/float.c:945)
  - [float48le](float48le.md) (mixed precision comparison)
  - [float84le](float84le.md) (mixed precision comparison)

## Notes and Other Information
- This is an inline function defined in the header file for performance optimization
- Widely used in PostgreSQL's geometric data types and GiST indexing operations
- Implements consistent NaN handling semantics across the PostgreSQL system
- Used in both pure double-precision comparisons and mixed-precision scenarios
- Critical for proper functioning of spatial indexing and geometric computations