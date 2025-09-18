# int28div

## Location
[src/backend/utils/adt/int8.c:1155-1183](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L1155-L1183)

## Overview
Divides a 16-bit integer (smallint) by a 64-bit integer (bigint) and returns the result as a 64-bit integer.

## Definition


## Detailed Description
This function implements division of a 2-byte integer by an 8-byte integer in PostgreSQL. The operation computes arg1 / arg2 where arg1 is a 16-bit integer and arg2 is a 64-bit integer. The function handles the critical edge case of division by zero by throwing an appropriate error.

Unlike some other integer arithmetic functions in PostgreSQL, this function does not need overflow checking because dividing a smaller integer by a larger integer cannot result in overflow - the result will always be within the range of the 64-bit result type.

## Parameters / Member Variables
-  (int16): The dividend (16-bit integer from first function argument)
-  (int64): The divisor (64-bit integer from second function argument)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT16 (extracts 16-bit argument)
  - PG_GETARG_INT64 (extracts 64-bit argument)
  - PG_RETURN_INT64 (returns 64-bit result)
  - ereport (error reporting)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- The function is defined in src/backend/utils/adt/int8.c:1155-1183
- Only checks for division by zero; no overflow checking is needed
- The function name follows PostgreSQL's convention where 'int2' refers to 16-bit integers, 'int8' refers to 64-bit integers, and 'div' indicates division
- Automatically promotes the smaller integer type to 64-bit before computation
- The comment 'No overflow is possible' reflects that dividing a 16-bit value by a 64-bit value cannot exceed 64-bit range
- Note the order of operands: this computes (smallint / bigint), not (bigint / smallint)
- Reports DIVISION_BY_ZERO error when the divisor is zero