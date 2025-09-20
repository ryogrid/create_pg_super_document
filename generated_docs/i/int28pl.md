# int28pl

## Location
[src/backend/utils/adt/int8.c:1113-1126](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L1113-L1126)

## Overview
Adds a 16-bit integer (smallint) to a 64-bit integer (bigint) and returns the result as a 64-bit integer.

## Definition

```c
Datum
int28pl(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements addition of a 2-byte integer with an 8-byte integer in PostgreSQL. The function performs safe addition by using PostgreSQL's overflow-checking arithmetic functions. The 16-bit argument is implicitly converted to 64-bit before the addition operation to ensure proper type compatibility.

The function uses PostgreSQL's pg_add_s64_overflow utility to detect overflow conditions during addition and reports an error if the result would exceed the range of a 64-bit signed integer.

## Parameters / Member Variables
-  (int16): The first addend (16-bit integer from first function argument)  
-  (int64): The second addend (64-bit integer from second function argument)
-  (int64): The computed sum

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT16 (extracts 16-bit argument)
  - PG_GETARG_INT64 (extracts 64-bit argument)
  - [pg_add_s64_overflow](../p/pg_add_s64_overflow.md) (safe 64-bit addition with overflow detection)
  - PG_RETURN_INT64 (returns 64-bit result)
  - ereport (error reporting)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- The function is defined in src/backend/utils/adt/int8.c:1113-1126
- Uses PostgreSQL's overflow-safe arithmetic functions to prevent integer overflow
- The function name follows PostgreSQL's convention where 'int2' refers to 16-bit integers, 'int8' refers to 64-bit integers, and 'pl' indicates plus/addition
- Automatically promotes the smaller integer type to match the larger one before computation
- Reports NUMERIC_VALUE_OUT_OF_RANGE error when overflow occurs