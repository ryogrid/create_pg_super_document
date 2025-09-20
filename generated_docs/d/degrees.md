# degrees

## Location
[src/backend/utils/adt/float.c:2554-2565](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L2554-L2565)

## Overview
The  function converts an angle measurement from radians to degrees, providing a simple unit conversion for trigonometric calculations.

## Definition

```c
Datum
degrees(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function performs a straightforward mathematical conversion from radians to degrees using the standard conversion factor. It divides the input radian value by the constant  (which represents π/180) to obtain the equivalent degree measurement. The function uses PostgreSQL's  function to ensure proper floating-point division with appropriate error handling.

## Parameters / Member Variables
- : The input angle in radians (float8 type extracted via PG_GETARG_FLOAT8)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8: Extracts float8 argument from function call
  - RADIANS_PER_DEGREE: Constant representing π/180 for radian-to-degree conversion
  - [float8_div](../f/float8_div.md): PostgreSQL's float8 division function with error handling
- Called from: No direct references found in the codebase

## Notes and Other Information
- Simple mathematical conversion: degrees = radians / (π/180)
- Uses PostgreSQL's division function to handle potential division errors
- Located in src/backend/utils/adt/float.c:2554-2565
- Complementary function to  which performs the inverse conversion