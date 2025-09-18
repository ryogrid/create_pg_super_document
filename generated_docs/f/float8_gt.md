# float8_gt

## Location
src/include/utils/float.h: 316 - 321

## Overview
Compares two double-precision floating-point values to determine if the first value is greater than the second, with proper NaN handling according to IEEE 754 standards.

## Definition
```c
static inline bool
float8_gt(const float8 val1, const float8 val2)
```

## Detailed Description
This inline function implements the "greater than" comparison for double-precision floating-point numbers (float8). Like its single-precision counterpart float4_gt, it follows IEEE 754 semantics where NaN is treated as the largest possible value. The function returns false if val2 is NaN (nothing can be greater than NaN), but returns true if val1 is NaN and val2 is not (NaN > non-NaN).

The implementation mirrors float4_gt with the same logical structure: first verify that val2 is not NaN, then check if either val1 is NaN OR val1 is numerically greater than val2.

## Parameters / Member Variables
- `val1`: The first double-precision floating-point value (left operand of the > comparison)
- `val2`: The second double-precision floating-point value (right operand of the > comparison)

## Dependencies
- Functions called/Symbols referenced:
  - float8 (type definition for double-precision float)
  - isnan (standard library function for NaN detection)
- Called from (representative examples):
  - adjustBox (GiST index bounding box adjustments)
  - gist_box_picksplit (GiST index splitting for geometric data)
  - float8larger (max function implementation)
  - float8_cmp_internal (internal comparison function)
  - float8gt (wrapper function in src/backend/utils/adt/float.c:954)
  - box_construct (geometric bounding box construction)
  - make_bound_box (bounding box creation for geometric types)
  - float8_max (maximum value computation macro)

## Notes and Other Information
- This is an inline function defined in the header file for performance optimization
- Extensively used in geometric data types, spatial indexing (GiST), and aggregate operations
- Critical for PostgreSQL's geometric types like boxes, polygons, and spatial calculations
- Implements consistent NaN ordering semantics across the PostgreSQL system
- Used in both pure double-precision comparisons and mixed-precision scenarios
- Essential for proper functioning of spatial indexes, sorting operations, and MAX aggregates