# int8smaller

## Location
[src/backend/utils/adt/int8.c:878-889](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L878-L889)

## Overview
Returns the smaller of two 64-bit signed integers, implementing the minimum function for bigint data types.

## Definition
Datum int8smaller(PG_FUNCTION_ARGS)

## Detailed Description
int8smaller compares two 64-bit signed integers and returns the smaller value. This function implements the minimum operation for PostgreSQL's bigint data type, using a simple conditional expression to determine which of the two input values is lesser. The function follows PostgreSQL's standard function calling convention and returns the result as a Datum.

## Parameters / Member Variables
- PG_FUNCTION_ARGS: Contains two int64 arguments to be compared
  - arg1: First 64-bit signed integer retrieved via PG_GETARG_INT64(0)
  - arg2: Second 64-bit signed integer retrieved via PG_GETARG_INT64(1)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64
  - PG_RETURN_INT64
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
This function provides the underlying implementation for PostgreSQL's least() function when applied to bigint values, or for any SQL context where the minimum of two bigint values is needed. The implementation uses a simple ternary operator for the comparison. The function is defined in src/backend/utils/adt/int8.c:878-889.