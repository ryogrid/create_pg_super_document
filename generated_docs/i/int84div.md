# int84div

## Location
[src/backend/utils/adt/int8.c:932-970](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L932-L970)

## Overview
The int84div function performs division of a 64-bit integer by a 32-bit integer, returning the result as a 64-bit integer with proper error handling for division by zero and overflow cases.

## Definition
```c
Datum int84div(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the division operation for PostgreSQL's bigint (int8) and integer (int4) data types. It takes a 64-bit integer as the dividend and divides it by a 32-bit integer divisor. The function includes comprehensive error handling for division by zero and the special case of INT64_MIN / -1, which would cause overflow on two's-complement machines. The function handles the -1 divisor case by treating it as negation, with special overflow detection for the minimum 64-bit value.

## Parameters / Member Variables
- `arg1`: 64-bit integer (bigint) - the dividend to be divided
- `arg2`: 32-bit integer (int) - the divisor
- `result`: 64-bit integer - stores the computed quotient

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64 - retrieves the first 64-bit integer argument
  - PG_GETARG_INT32 - retrieves the second 32-bit integer argument  
  - PG_INT64_MIN - constant representing the minimum 64-bit integer value
  - PG_RETURN_INT64 - returns the 64-bit result
  - PG_RETURN_NULL - returns null in unreachable code path
  - ereport - reports errors for division by zero and overflow
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's int8 (bigint) arithmetic operations module
- Includes explicit division by zero checking with appropriate error reporting
- Handles the problematic INT64_MIN / -1 case which can't be represented in two's-complement arithmetic
- The division by -1 is optimized to negation for better performance and overflow handling
- Error handling follows PostgreSQL's standard error reporting mechanism
- Located in src/backend/utils/adt/int8.c:932-970
- Contains compiler hints to ensure proper optimization and avoid undefined behavior