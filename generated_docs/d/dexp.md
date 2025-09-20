# dexp

## Location
[src/backend/utils/adt/float.c:1637-1682](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L1637-L1682)

## Overview
The dexp function implements PostgreSQL's exponential function, returning e raised to the power of the input argument.

## Definition

```c
Datum
dexp(PG_FUNCTION_ARGS)
```
## Detailed Description
The dexp function is PostgreSQL's implementation of the exponential function (exp). It takes a single float8 argument and returns e^arg1. The function includes comprehensive error handling for special floating-point values including NaN and infinity cases, following POSIX standards. It also implements overflow and underflow detection to ensure robust behavior across different platforms.

The function explicitly handles:
- NaN inputs (returns NaN)
- Positive infinity (returns positive infinity) 
- Negative infinity (returns 0 per POSIX specification)
- Overflow conditions (throws float_overflow_error)
- Underflow conditions (throws float_underflow_error)

## Parameters / Member Variables
- : The float8 input value for which to compute the exponential function

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8 (to extract input argument)
  - isnan (to check for NaN values)
  - isinf (to check for infinity values)
  - exp (standard C library exponential function)
  - [float_overflow_error](../f/float_overflow_error.md) (PostgreSQL error handling)
  - [float_underflow_error](../f/float_underflow_error.md) (PostgreSQL error handling)
- Called from: 
  - No direct references found in the codebase (likely called through SQL function dispatch)

## Notes and Other Information
- Located in src/backend/utils/adt/float.c:1637-1682
- This function is part of PostgreSQL's floating-point arithmetic operations
- Includes platform-independent handling of edge cases since different platforms may handle exp() differently
- Uses errno checking and explicit result validation for robust error detection
- The function follows PostgreSQL's standard function interface using PG_FUNCTION_ARGS and PG_RETURN_FLOAT8