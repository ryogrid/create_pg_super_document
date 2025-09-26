# pg_hypot

## Location
[src/backend/utils/adt/geo_ops.c:5519-5562](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L5519-L5562)

## Overview
A numerically stable implementation of the hypotenuse function that computes sqrt(x² + y²) with enhanced precision and overflow protection for PostgreSQL's geometric operations.

## Definition

```c
float8
pg_hypot(float8 x, float8 y)
```
## Detailed Description
This function implements a numerically stable algorithm to compute the hypotenuse of a right triangle with sides x and y. Instead of the naive formula sqrt(x² + y²), it uses a rearranged formula that factors out the larger value to prevent overflow and improve precision:

**Mathematical transformation:**
- sqrt(x² + y²) = sqrt(x²(1 + y²/x²)) = x × sqrt(1 + y²/x²) = x × sqrt(1 + (y/x)²)

**Key features:**
- Handles special IEEE floating-point values (INF, NaN) correctly
- Prevents overflow by factoring out the larger operand
- Maintains high precision for large values
- Conforms to IEEE Std 1003.1 and GLIBC standards
- Expected to be replaced by C99 hypot() function in future versions

**Algorithm steps:**
1. Handle special cases (infinity and NaN)
2. Take absolute values of both inputs
3. Ensure x ≥ y by swapping if necessary
4. Handle y = 0 case directly
5. Apply the numerically stable formula
6. Check for overflow/underflow in result

## Parameters / Member Variables
- : First operand (floating-point value representing one side of triangle)
- : Second operand (floating-point value representing other side of triangle)

## Dependencies
- Functions called/Symbols referenced:
  - isinf (checks for infinity)
  - [get_float8_infinity](../g/get_float8_infinity.md) (returns floating-point infinity)
  - isnan (checks for NaN)
  - [get_float8_nan](../g/get_float8_nan.md) (returns floating-point NaN)
  - [float_overflow_error](../f/float_overflow_error.md) (handles overflow errors)
  - [float_underflow_error](../f/float_underflow_error.md) (handles underflow errors)
- Called from (representative examples):
  - HYPOT (macro definition)
  - PG_RETURN_CIRCLE_P (circle geometric operations)

## Notes and Other Information
- Designed as a more robust alternative to the standard hypot() function
- Critical for distance calculations in PostgreSQL's geometric data types
- Handles edge cases that could cause numerical instability in naive implementations
- The IEEE compliance ensures consistent behavior with hypot(inf,nan) returning INF rather than NaN
- Part of PostgreSQL's geometric operations infrastructure in geo_ops.c
- Uses float8 type which is PostgreSQL's double-precision floating-point type