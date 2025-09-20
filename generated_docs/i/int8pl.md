# int8pl

## Location
[src/backend/utils/adt/int8.c:462-475](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L462-L475)

## Overview
The int8pl function implements addition operation for two 64-bit signed integers (bigint) in PostgreSQL, with overflow detection and error handling.

## Definition

```c
Datum
int8pl(PG_FUNCTION_ARGS)
```
## Detailed Description
This function performs addition of two 64-bit signed integer arguments. It extracts both input arguments using PostgreSQL's function argument macros, performs overflow-safe addition using the pg_add_s64_overflow utility function, and returns the result. If overflow is detected during the addition operation, the function reports an error with an appropriate error code. This function is part of PostgreSQL's arithmetic operators for the bigint data type and ensures mathematical correctness by preventing silent overflow conditions.

## Parameters / Member Variables
- The function uses PostgreSQL's standard function argument mechanism where:

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64 (macro for extracting int64 arguments)
  - [pg_add_s64_overflow](../p/pg_add_s64_overflow.md) (overflow-safe addition function)
  - PG_RETURN_INT64 (macro for returning int64 result)
  - ereport (error reporting function)
- Called from: 
  - This function is typically invoked through PostgreSQL's function call mechanism for bigint addition operations

## Notes and Other Information
- Uses pg_add_s64_overflow for safe arithmetic that detects overflow conditions before they occur
- Error handling follows PostgreSQL conventions by using ereport with ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE
- Located in src/backend/utils/adt/int8.c:462-475
- This is a binary arithmetic operator that requires two int64 operands