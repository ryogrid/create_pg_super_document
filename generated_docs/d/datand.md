# datand

## Location
[src/backend/utils/adt/float.c:2175-2206](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L2175-L2206)

## Overview
The  function computes the inverse tangent (arctangent) of a floating-point value and returns the result in degrees rather than radians.

## Definition

```c
Datum
datand(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the PostgreSQL SQL function  with degree output. It takes a single floating-point argument and computes its inverse tangent, returning the result in degrees within the range [-90, 90]. The implementation includes several notable features:

- Accepts any finite or infinite floating-point input
- Handles NaN inputs by returning NaN as per POSIX specification  
- Converts from radians to degrees using a precise scaling factor based on  (the arctangent of 1.0)
- Ensures that  returns exactly 45.0 degrees for mathematical precision
- Uses the standard C library  function for the core computation

## Parameters / Member Variables
- : The floating-point input value for which to compute the inverse tangent in degrees (accepts any real number including infinity)
- : Local volatile variable storing the result of  in radians
- : The final result converted to degrees

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8: Extracts float8 argument from function call
  - isnan: Checks if input is Not-a-Number
  - [get_float8_nan](../g/get_float8_nan.md): Returns NaN value for float8
  - INIT_DEGREE_CONSTANTS: Initializes degree conversion constants including 
  - atan: Standard C library arctangent function (returns radians)
  - isinf: Checks if result is infinite
  - [float_overflow_error](../f/float_overflow_error.md): Reports overflow error
- Called from (representative examples):
  - No direct references found (likely called via SQL function dispatch)

## Notes and Other Information
- Unlike inverse sine and cosine functions, inverse tangent accepts infinite inputs and always produces finite results
- The conversion formula  ensures precise degree conversion while maintaining the property that 
- Part of PostgreSQL's mathematical function library located in src/backend/utils/adt/float.c:2175-2206
- Implements the SQL standard ATAN function with degree output rather than radian output
- The use of volatile storage for  may help ensure consistent floating-point behavior across different compiler optimizations