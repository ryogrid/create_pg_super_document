# radians

## Location
[src/backend/utils/adt/float.c:2576-2590](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L2576-L2590)

## Overview
The  function converts an angle measurement from degrees to radians, providing the inverse unit conversion to the  function for trigonometric calculations.

## Definition

```c
Datum
radians(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function performs a mathematical conversion from degrees to radians using the standard conversion factor. It multiplies the input degree value by the constant  (which represents π/180) to obtain the equivalent radian measurement. The function uses PostgreSQL's  function to ensure proper floating-point multiplication with appropriate error handling. This conversion is essential for trigonometric functions that expect radian inputs.

## Parameters / Member Variables
- : The input angle in degrees (float8 type extracted via PG_GETARG_FLOAT8)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8: Extracts float8 argument from function call
  - RADIANS_PER_DEGREE: Constant representing π/180 for degree-to-radian conversion
  - [float8_mul](../f/float8_mul.md): PostgreSQL's float8 multiplication function with error handling
- Called from: No direct references found in the codebase

## Notes and Other Information
- Mathematical conversion: radians = degrees × (π/180)
- Uses PostgreSQL's multiplication function to handle potential arithmetic errors
- Located in src/backend/utils/adt/float.c:2576-2590
- Complementary function to  which performs the inverse conversion
- Essential for preparing degree inputs for standard trigonometric functions that expect radian arguments