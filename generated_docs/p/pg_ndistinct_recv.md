# pg_ndistinct_recv

## Location
[src/backend/statistics/mvdistinct.c:392-407](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/mvdistinct.c#L392-L407)

## Overview
A PostgreSQL binary input function for the pg_ndistinct data type that explicitly rejects binary input operations by throwing an error.

## Definition


## Detailed Description
The pg_ndistinct_recv function serves as the binary input routine for the pg_ndistinct data type in PostgreSQL. However, instead of accepting binary input, this function deliberately throws a FEATURE_NOT_SUPPORTED error, indicating that binary input operations are not supported for the pg_ndistinct type. This is a design decision to prevent users from directly creating pg_ndistinct values through binary input methods, as these statistics are typically generated internally by PostgreSQL's statistics collection processes.

## Parameters / Member Variables
- Input parameter (via PG_FUNCTION_ARGS):
  - Binary input data (not processed due to error)

## Dependencies
- Functions called/Symbols referenced:
  - ereport (PostgreSQL error reporting function)
  - [errcode](../e/errcode.md) (error code specification macro)
  - [errmsg](../e/errmsg.md) (error message formatting macro)  
  - PG_RETURN_VOID (macro to return void Datum)
- Error codes used:
  - ERRCODE_FEATURE_NOT_SUPPORTED (PostgreSQL error code)
- Called from:
  - No direct references found (used as type input function)

## Notes and Other Information
- This function intentionally prevents binary input for pg_ndistinct type
- The pg_ndistinct type is designed to be created only through internal statistics processes
- The PG_RETURN_VOID() at the end is included only to keep the compiler quiet, as the ereport() call will never return
- Located in src/backend/statistics/mvdistinct.c:392-407