# dtan

## Location
[src/backend/utils/adt/float.c:1958-2011](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L1958-L2011)

## Overview
A PostgreSQL function that calculates the tangent of a given floating-point number in radians.

## Definition

```c
Datum
dtan(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function computes the tangent of a floating-point argument expressed in radians. It follows POSIX specifications for handling special cases like NaN and infinite inputs. The function uses the standard C library's  function internally and includes proper error handling for out-of-range inputs. Unlike some trigonometric functions, it does not check for overflow since  naturally evaluates to infinity.

## Parameters / Member Variables
- : A float8 (double precision) input value representing an angle in radians

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8 (to extract the float8 argument)
  - isnan (to check for NaN input)
  - get_float8_nan (to return NaN when input is NaN)
  - tan (standard C library tangent function)
  - isinf (to check for infinite input)
  - ereport (for error reporting)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Returns NaN if the input is NaN, following POSIX specifications
- Throws an error for infinite inputs to prevent undefined behavior
- Does not check for overflow since tan(π/2) legitimately equals infinity
- Located in src/backend/utils/adt/float.c:1958-2011
- Uses errno to detect mathematical errors from the tan() function