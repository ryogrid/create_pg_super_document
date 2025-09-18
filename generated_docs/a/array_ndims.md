# array_ndims

## Location
[src/backend/utils/adt/arrayfuncs.c:1652-1667](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L1652-L1667)

## Overview
Returns the number of dimensions of a PostgreSQL array, providing essential metadata about the array's structure.

## Definition


## Detailed Description
The  function is a PostgreSQL built-in function that examines an array and returns its dimensionality (number of dimensions). It performs basic sanity checks to ensure the input is a valid array structure before returning the dimension count. If the array has an invalid dimension count (≤ 0 or > MAXDIM), the function returns NULL instead of an invalid value.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Array argument accessed via  - the input array to examine

## Dependencies
- Functions called/Symbols referenced:
  -  - macro to extract array argument
  -  - macro to get number of dimensions from array header
  -  - generic array type structure
  -  - maximum allowed array dimensions constant
  -  - macro to return 32-bit integer result
  -  - macro to return NULL value
- Called from (representative examples):
  - SQL queries using  function
  - Array processing routines requiring dimension information

## Notes and Other Information
- Returns NULL for arrays with invalid dimension counts (≤ 0 or > MAXDIM)
- Part of PostgreSQL's array manipulation function suite
- Defined in src/backend/utils/adt/arrayfuncs.c:1652-1667
- Thread-safe and read-only operation on array metadata