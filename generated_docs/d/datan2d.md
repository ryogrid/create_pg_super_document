# datan2d

## Location
[src/backend/utils/adt/float.c:2207-2244](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L2207-L2244)

## Overview
The  function computes the two-argument inverse tangent (atan2) of two floating-point values and returns the result in degrees rather than radians, providing the angle in the correct quadrant.

## Definition


## Detailed Description
This function implements the PostgreSQL SQL function  with degree output. It takes two floating-point arguments (y and x coordinates) and computes , returning the result in degrees within the range [-180, 180]. The two-argument arctangent function is superior to the single-argument version because it:

- Determines the correct quadrant for the angle by considering the signs of both arguments
- Handles special cases where x=0 (which would cause division by zero in y/x)
- Maps all possible input combinations to a full 360-degree range (returned as [-180, 180])
- Accepts infinite inputs while always producing finite results
- Returns NaN if either input is NaN, following POSIX specifications

The implementation converts from radians to degrees using the same scaling approach as , based on .

## Parameters / Member Variables
- : The y-coordinate (first argument) for the two-argument arctangent computation
- : The x-coordinate (second argument) for the two-argument arctangent computation  
- : Local volatile variable storing the result of  in radians
- : The final result converted to degrees

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8: Extracts float8 arguments from function call (called twice)
  - isnan: Checks if either input is Not-a-Number
  - get_float8_nan: Returns NaN value for float8
  - INIT_DEGREE_CONSTANTS: Initializes degree conversion constants including 
  - atan2: Standard C library two-argument arctangent function (returns radians)
  - isinf: Checks if result is infinite
  - [float_overflow_error](../f/float_overflow_error.md): Reports overflow error
- Called from (representative examples):
  - No direct references found (likely called via SQL function dispatch)

## Notes and Other Information
- The function signature follows the mathematical convention where  computes the angle from the positive x-axis to the point (x, y)
- Unlike single-argument ,  can distinguish between all four quadrants and handle edge cases like x=0
- The comment notes potential concerns about the scaling constant approach and suggests this might need refinement for guaranteed exact results in specific cases
- Part of PostgreSQL's mathematical function library located in src/backend/utils/adt/float.c:2207-2244
- Implements the SQL standard ATAN2 function with degree output rather than radian output
- The use of volatile storage for  helps ensure consistent floating-point behavior across compiler optimizations