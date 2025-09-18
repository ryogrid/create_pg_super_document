# int8random

## Location
[src/backend/utils/adt/pseudorandomfuncs.c:150-173](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pseudorandomfuncs.c#L150-L173)

## Overview
Generates a random 64-bit integer uniformly distributed within a specified range.

## Definition


## Detailed Description
The  function is a PostgreSQL built-in function that generates pseudo-random 64-bit integers (bigint type) within a user-specified range. It takes two parameters defining the lower and upper bounds (inclusive) and returns a uniformly distributed random value between these bounds. The function validates that the lower bound does not exceed the upper bound before generating the random number. It uses PostgreSQL's internal pseudo-random number generator (PRNG) which is initialized on first use.

## Parameters / Member Variables
-  (int64): The lower bound (inclusive) for the random number generation
-  (int64): The upper bound (inclusive) for the random number generation

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64: Extracts int64 arguments from PostgreSQL function call
  - [initialize_prng](initialize_prng.md): Initializes the pseudo-random number generator state
  - [pg_prng_int64_range](../p/pg_prng_int64_range.md): Generates a random int64 within the specified range
  - PG_RETURN_INT64: Returns an int64 value from PostgreSQL function
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- Raises an ERROR with ERRCODE_INVALID_PARAMETER_VALUE if rmin > rmax
- Uses PostgreSQL's internal PRNG state for consistent random number generation
- Part of PostgreSQL's pseudo-random functions subsystem located in pseudorandomfuncs.c
- The function is designed to be called from SQL as a built-in function