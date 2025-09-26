# dtand

## Location
[src/backend/utils/adt/float.c:2488-2553](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L2488-L2553)

## Overview
The  function computes the tangent of an angle specified in degrees, providing accurate trigonometric calculations with special handling for edge cases and domain reduction.

## Definition

```c
Datum
dtand(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function implements the tangent function for degree-based input, following POSIX specifications for special value handling. It uses a sophisticated domain reduction algorithm to map arbitrary degree inputs to the range [0,90] degrees, then computes the tangent using the ratio of  and  functions. The implementation includes special handling for NaN inputs, infinite inputs, and ensures portability by normalizing minus zero to plain zero. The function leverages the mathematical properties of tangent to reduce computational complexity while maintaining accuracy.

## Parameters / Member Variables
- : The input angle in degrees (float8 type extracted via PG_GETARG_FLOAT8)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8: Extracts float8 argument from function call
  - isnan: Checks for NaN input values
  - [get_float8_nan](../g/get_float8_nan.md): Returns NaN value for float8 type  
  - isinf: Checks for infinite input values
  - INIT_DEGREE_CONSTANTS: Initializes degree calculation constants
  - [sind_q1](../s/sind_q1.md): Computes sine for first quadrant (0-90 degrees)
  - [cosd_q1](../c/cosd_q1.md): Computes cosine for first quadrant (0-90 degrees)
- Called from: No direct references found in the codebase

## Notes and Other Information
- Returns NaN for NaN input per POSIX specification
- Throws error for infinite input values
- Uses domain reduction to map input to [0,90] degree range for computation
- Applies sign corrections based on quadrant properties of tangent function
- Forces minus zero results to plain zero for portability
- Does not check for overflow since tand(90°) legitimately equals infinity
- Located in src/backend/utils/adt/float.c:2488-2553