# int8shr

## Location
[src/backend/utils/adt/int8.c:1228-1240](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L1228-L1240)

## Overview
Performs a right bit-shift operation on a 64-bit integer (int8) by a specified number of positions.

## Definition


## Detailed Description
The int8shr function implements the bit-shift right operation for PostgreSQL's 8-byte integer type (int8/bigint). It takes two arguments: the first is the 64-bit integer value to be shifted, and the second is a 32-bit integer specifying the number of positions to shift right. The function performs an arithmetic right shift using the C >> operator, which preserves the sign bit for negative numbers.

## Parameters / Member Variables
-  (int64): The 64-bit integer value to be right-shifted
-  (int32): The number of bit positions to shift right

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64 (macro for extracting 64-bit integer argument)
  - PG_RETURN_INT64 (macro for returning 64-bit integer result)
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- This function is typically invoked through PostgreSQL's SQL interface using the right-shift operator (#>>) for bigint values
- The right shift is arithmetic, meaning it preserves the sign bit for negative numbers
- Located in src/backend/utils/adt/int8.c, which contains arithmetic operations for 8-byte integers
- Part of PostgreSQL's built-in function system for bigint data type operations