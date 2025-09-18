# dlog1

## Location
[src/backend/utils/adt/float.c:1683-1714](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L1683-L1714)

## Overview
The dlog1 function implements PostgreSQL's natural logarithm function, returning the natural logarithm (base e) of the input argument.

## Definition
```c
Datum dlog1(PG_FUNCTION_ARGS)
```

## Detailed Description
The dlog1 function is PostgreSQL's implementation of the natural logarithm function (ln). It takes a single float8 argument and returns ln(arg1). The function includes SQL standard-compliant error handling for invalid inputs, specifically zero and negative values, which are mathematically undefined for the logarithm function. It also implements overflow and underflow detection for edge cases.

The function explicitly handles:
- Zero input (throws ERRCODE_INVALID_ARGUMENT_FOR_LOG error)
- Negative input (throws ERRCODE_INVALID_ARGUMENT_FOR_LOG error) 
- Overflow conditions (throws float_overflow_error)
- Underflow conditions (throws float_underflow_error)

## Parameters / Member Variables
- `arg1`: The float8 input value for which to compute the natural logarithm

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8 (to extract input argument)
  - ereport (PostgreSQL error reporting system)
  - [errcode](../e/errcode.md) (error code specification)
  - [errmsg](../e/errmsg.md) (error message specification)
  - log (standard C library natural logarithm function)
  - isinf (to check for infinity values)
  - [float_overflow_error](../f/float_overflow_error.md) (PostgreSQL error handling)
  - [float_underflow_error](../f/float_underflow_error.md) (PostgreSQL error handling)
- Called from: 
  - No direct references found in the codebase (likely called through SQL function dispatch)

## Notes and Other Information
- Located in src/backend/utils/adt/float.c:1683-1714
- This function is part of PostgreSQL's floating-point arithmetic operations
- Follows SQL standard requirements for error codes when dealing with invalid logarithm inputs
- Uses ERRCODE_INVALID_ARGUMENT_FOR_LOG as mandated by the SQL standard
- The function follows PostgreSQL's standard function interface using PG_FUNCTION_ARGS and PG_RETURN_FLOAT8
- Domain restrictions: arg1 must be > 0