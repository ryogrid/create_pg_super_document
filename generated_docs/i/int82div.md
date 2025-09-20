# int82div

## Location
[src/backend/utils/adt/int8.c:1074-1112](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L1074-L1112)

## Overview
Divides a 64-bit integer (bigint) by a 16-bit integer (smallint) and returns the result as a 64-bit integer.

## Definition

```c
Datum
int82div(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements division of an 8-byte integer by a 2-byte integer in PostgreSQL. It handles several edge cases to ensure safe operation:

1. **Division by zero protection**: Throws an error when the divisor is zero
2. **Overflow protection**: Handles the special case of INT64_MIN / -1, which would cause overflow in two's-complement arithmetic
3. **Optimization for division by -1**: Recognizes that division by -1 is equivalent to negation

The function uses PostgreSQL's function call interface macros to extract arguments and return results.

## Parameters / Member Variables
-  (int64): The dividend (64-bit integer from first function argument)
-  (int16): The divisor (16-bit integer from second function argument)
-  (int64): The computed quotient

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64 (extracts 64-bit argument)
  - PG_GETARG_INT16 (extracts 16-bit argument)
  - PG_INT64_MIN (minimum 64-bit integer constant)
  - PG_RETURN_INT64 (returns 64-bit result)
  - ereport (error reporting)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- The function is defined in src/backend/utils/adt/int8.c:1074-1112
- Uses PostgreSQL's error reporting system for division by zero and overflow conditions
- Implements safe integer division with proper edge case handling
- The function follows PostgreSQL's naming convention where 'int8' refers to 64-bit integers and '2' refers to 16-bit integers
- No overflow is possible for normal division cases due to the smaller divisor size