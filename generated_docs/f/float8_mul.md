# float8_mul

## Location
[src/include/utils/float.h:208-221](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/float.h#L208-L221)

## Overview
Performs multiplication of two double-precision floating-point numbers (float8) with overflow and underflow error checking.

## Definition

```c
static inline float8
float8_mul(const float8 val1, const float8 val2)
```
## Detailed Description
The  function multiplies two  (double-precision floating-point) values and returns the result with appropriate error handling. This is an inline function defined in the header file for performance optimization. Similar to , this function performs standard multiplication but includes additional checks to detect and handle floating-point overflow and underflow conditions, which are critical for maintaining data integrity in database operations.

The function uses the  macro to optimize branch prediction, as overflow and underflow conditions are expected to be rare in normal operation. This function is widely used throughout PostgreSQL for geometric calculations, financial operations, and general floating-point arithmetic.

## Parameters / Member Variables
- : The first double-precision floating-point operand (multiplicand)
- : The second double-precision floating-point operand (multiplier)

## Dependencies
- Functions called/Symbols referenced:
  - : Standard C library function to check for infinity
  - : PostgreSQL error handler for floating-point overflow
  - : PostgreSQL error handler for floating-point underflow
  - : Type alias for double-precision floating-point (double)
- Called from (representative examples):
  - : Main SQL-callable multiplication function in src/backend/utils/adt/float.c:786
  - : Money type multiplication in src/backend/utils/adt/cash.c:119
  - : Angle conversion function in src/backend/utils/adt/float.c:2580
  - : Geometric point multiplication in src/backend/utils/adt/geo_ops.c:4160
  - Various geometric operations in geo_ops.c for lines, circles, and other shapes

## Notes and Other Information
- This is an inline function for performance, defined in src/include/utils/float.h:208-221
- The function checks for overflow by detecting when the result is infinite but neither input was infinite
- Underflow is detected when the result is zero but both inputs are non-zero
- The  macro is used to hint to the compiler that error conditions are rare
- This function is part of PostgreSQL's type system implementation for the  SQL data type (float8)
- Extensively used in PostgreSQL's geometric data types and operations
- The function follows PostgreSQL's convention of throwing errors rather than returning special values for exceptional conditions
- More widely used than  due to the prevalence of double-precision arithmetic in database operations