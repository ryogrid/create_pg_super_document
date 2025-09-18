# float8_mi

## Location
src/include/utils/float.h: 182 - 193

## Overview
Performs double-precision floating-point subtraction with overflow detection and error reporting.

## Definition
```c
static inline float8 float8_mi(const float8 val1, const float8 val2)
```

## Detailed Description
This inline function implements safe double-precision floating-point subtraction by performing the arithmetic operation and checking for overflow conditions. The function subtracts val2 from val1 and validates that the result hasn't overflowed to infinity. If both input values are finite but the result is infinite, it indicates an overflow condition and triggers an error. The function is designed to catch arithmetic overflow while allowing legitimate infinite results (when at least one input is already infinite) to pass through unchanged. This is the double-precision equivalent of float4_mi.

## Parameters / Member Variables
- `val1`: First double-precision floating-point operand (minuend)
- `val2`: Second double-precision floating-point operand (subtrahend)

## Dependencies
- Functions called/Symbols referenced:
  - isinf (checks if value is infinite)
  - [float_overflow_error](float_overflow_error.md) (reports overflow error)
  - float8 (PostgreSQL's double-precision float type)
- Called from (representative examples):
  - [float8mi](float8mi.md) (src/backend/utils/adt/float.c:777)
  - [size_box](../s/size_box.md) (src/backend/access/gist/gistproc.c:88)
  - [box_wd](../b/box_wd.md) (src/backend/utils/adt/geo_ops.c:885)
  - [point_sub_point](../p/point_sub_point.md) (src/backend/utils/adt/geo_ops.c:4137)
  - [circle_distance](../c/circle_distance.md) (src/backend/utils/adt/geo_ops.c:5072)

## Notes and Other Information
- Defined as a static inline function in src/include/utils/float.h:182-193
- Part of PostgreSQL's floating-point arithmetic with overflow/underflow error reporting
- Cannot detect underflow in addition/subtraction due to rounding near underflow values
- Uses unlikely() macro for branch prediction optimization on overflow check
- Extensively used in geometric operations, GiST index operations, and spatial functions
- Implements IEEE 754 compliant arithmetic with PostgreSQL-specific error handling
- The function allows infinite results when at least one operand is already infinite
- Subtraction operation: result = val1 - val2