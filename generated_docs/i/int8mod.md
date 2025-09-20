# int8mod

## Location
[src/backend/utils/adt/int8.c:563-605](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L563-L605)

## Overview
Performs modulo operation on two 64-bit signed integers (bigint) with proper error handling for division by zero and edge case handling for problematic hardware behavior.

## Definition

```c
Datum
int8mod(PG_FUNCTION_ARGS)
```
## Detailed Description
The int8mod function implements the modulo (remainder) operation for PostgreSQL's bigint data type. It extracts two int64 arguments and computes arg1 % arg2. The function handles two special cases: division by zero (which raises an error) and modulo by -1, which on some machines can cause floating-point exceptions despite having a well-defined mathematical result of zero. The function works around this hardware quirk by explicitly returning 0 when the divisor is -1.

## Parameters / Member Variables
- : First operand (dividend) extracted as int64
- : Second operand (divisor) extracted as int64

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64 (extracts int64 arguments)
  - PG_RETURN_INT64 (returns int64 result)
  - PG_RETURN_NULL (returns NULL on error path)
  - ereport (error reporting)
  - [errcode](../e/errcode.md)/errmsg (error code and message macros)
- Called from:
  - No direct references found (likely called via PostgreSQL function dispatch system)

## Notes and Other Information
- Division by zero raises ERRCODE_DIVISION_BY_ZERO error
- Explicitly handles modulo by -1 to return 0, avoiding hardware exceptions on some platforms
- Uses unlikely() hint for the division by zero case to optimize common path
- Comment indicates this addresses floating-point exceptions on certain machines for INT64_MIN % -1
- No overflow is possible with modulo operation (unlike division)
- Compiler workaround comment for GCC optimization issues with unreachable code