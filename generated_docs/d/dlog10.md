# dlog10

## Location
[src/backend/utils/adt/float.c:1715-1747](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L1715-L1747)

## Overview
The dlog10 function implements PostgreSQL's base-10 logarithm function, returning the logarithm base 10 of the input argument.

## Definition
```c
Datum dlog10(PG_FUNCTION_ARGS)
```

## Detailed Description
The dlog10 function is PostgreSQL's implementation of the base-10 logarithm function (log10). It takes a single float8 argument and returns log10(arg1). The function includes error handling for invalid inputs that mirrors the natural logarithm function, using the same SQL error codes for consistency. Although the SQL standard does not define log(), PostgreSQL uses the same error codes as ln() for analogous error conditions.

The function explicitly handles:
- Zero input (throws ERRCODE_INVALID_ARGUMENT_FOR_LOG error)
- Negative input (throws ERRCODE_INVALID_ARGUMENT_FOR_LOG error) 
- Overflow conditions (throws float_overflow_error)
- Underflow conditions (throws float_underflow_error)

## Parameters / Member Variables
- `arg1`: The float8 input value for which to compute the base-10 logarithm

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8 (to extract input argument)
  - ereport (PostgreSQL error reporting system)
  - [errcode](../e/errcode.md) (error code specification)
  - [errmsg](../e/errmsg.md) (error message specification)
  - log10 (standard C library base-10 logarithm function)
  - isinf (to check for infinity values)
  - [float_overflow_error](../f/float_overflow_error.md) (PostgreSQL error handling)
  - [float_underflow_error](../f/float_underflow_error.md) (PostgreSQL error handling)
- Called from: 
  - No direct references found in the codebase (likely called through SQL function dispatch)

## Notes and Other Information
- Located in src/backend/utils/adt/float.c:1715-1747
- This function is part of PostgreSQL's floating-point arithmetic operations
- Uses the same error handling approach as dlog1 for consistency, even though SQL standard does not define log()
- Uses ERRCODE_INVALID_ARGUMENT_FOR_LOG for consistency with natural logarithm function
- The function follows PostgreSQL's standard function interface using PG_FUNCTION_ARGS and PG_RETURN_FLOAT8
- Domain restrictions: arg1 must be > 0