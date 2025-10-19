# float8_min

## Location
[src/include/utils/float.h:340-345](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/float.h#L340-L345)

## Overview
Returns the smaller of two double-precision floating-point numbers, with PostgreSQL's NaN handling semantics applied.

## Definition

```c
static inline float8
float8_min(const float8 val1, const float8 val2)
```
## Detailed Description
This inline function implements the minimum operation for double-precision floating-point numbers (float8). It uses the float8_lt comparison function to determine which value is smaller and returns that value. The function inherits PostgreSQL's NaN handling behavior from float8_lt, where NaN comparisons follow specific SQL standard semantics.

The function performs a simple conditional selection: if val1 is less than val2 according to float8_lt, it returns val1; otherwise, it returns val2. This function is extensively used in geometric operations and bounding box calculations.

## Parameters / Member Variables
- `val1`: The first double-precision floating-point value to compare
- `val2`: The second double-precision floating-point value to compare
## Dependencies
- Functions called/Symbols referenced:
  - [float8_lt](float8_lt.md) (for less-than comparison with NaN handling)
  - float4 (related floating-point type)
- Called from (representative examples):
  - [rt_box_union](../r/rt_box_union.md) (GiST R-tree operations)
  - [box_intersect](../b/box_intersect.md) (geometric box intersection)
  - [path_inter](../p/path_inter.md) (path intersection calculations)
  - [box_interpt_lseg](../b/box_interpt_lseg.md) (box-line segment intersection)
  - [boxes_bound_box](../b/boxes_bound_box.md) (bounding box calculations)

## Notes and Other Information
- This is an inline function defined in the header for performance optimization
- Heavily used in PostgreSQL's geometric data type operations
- Critical for bounding box calculations in spatial indexing (GiST)
- Relies on float8_lt for the actual comparison logic and NaN handling
- Part of the float8 family of utility functions for double-precision arithmetic
- The NaN behavior is consistent with PostgreSQL's floating-point semantics

## Simplified Source

```c
static inline float8
float8_min(const float8 val1, const float8 val2)
{
    // Return the smaller value using float8_lt comparison
    // Inherits NaN handling from float8_lt function
    return float8_lt(val1, val2) ? val1 : val2;
}
```