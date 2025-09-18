# float8_pl

## Location
src/include/utils/float.h: 158 - 169

## Overview
Performs double-precision floating-point addition with overflow detection and error reporting.

## Definition
```c
static inline float8 float8_pl(const float8 val1, const float8 val2)
```

## Detailed Description
This inline function implements safe double-precision floating-point addition by performing the arithmetic operation and checking for overflow conditions. The function adds two float8 values and validates that the result hasn't overflowed to infinity. If both input values are finite but the result is infinite, it indicates an overflow condition and triggers an error. The function is designed to catch arithmetic overflow while allowing legitimate infinite results (when at least one input is already infinite) to pass through unchanged. This is the double-precision equivalent of float4_pl.

## Parameters / Member Variables
- `val1`: First double-precision floating-point operand
- `val2`: Second double-precision floating-point operand

## Dependencies
- Functions called/Symbols referenced:
  - isinf (checks if value is infinite)
  - [float_overflow_error](float_overflow_error.md) (reports overflow error)
  - float8 (PostgreSQL's double-precision float type)
- Called from (representative examples):
  - [float8pl](float8pl.md) (src/backend/utils/adt/float.c:768)
  - [float8_combine](float8_combine.md) (src/backend/utils/adt/float.c:2912)
  - [float8_regr_combine](float8_regr_combine.md) (src/backend/utils/adt/float.c:3452)
  - [point_add_point](../p/point_add_point.md) (src/backend/utils/adt/geo_ops.c:4114)
  - [circle_distance](../c/circle_distance.md) (src/backend/utils/adt/geo_ops.c:5073)

## Notes and Other Information
- Defined as a static inline function in src/include/utils/float.h:158-169
- Part of PostgreSQL's floating-point arithmetic with overflow/underflow error reporting
- Cannot detect underflow in addition/subtraction due to rounding near underflow values
- Uses unlikely() macro for branch prediction optimization on overflow check
- Extensively used in geometric operations and aggregate functions
- Implements IEEE 754 compliant arithmetic with PostgreSQL-specific error handling
- The function allows infinite results when at least one operand is already infinite