# int4random

## Location
src/backend/utils/adt/pseudorandomfuncs.c: 126 - 149

## Overview
Returns a pseudo-random 32-bit integer uniformly distributed within a specified range [rmin, rmax].

## Definition
```c
Datum int4random(PG_FUNCTION_ARGS)
```

## Detailed Description
This PostgreSQL function generates pseudo-random 32-bit signed integers uniformly distributed within a user-specified inclusive range. The function takes two parameters: a lower bound (rmin) and upper bound (rmax), and returns a random integer where rmin ≤ result ≤ rmax.

The function includes input validation to ensure the lower bound is less than or equal to the upper bound, raising a PostgreSQL error if this condition is violated. It uses the internal `pg_prng_int64_range()` function to generate the random value, which handles the complex mathematics required for unbiased range mapping, then casts the result to a 32-bit integer.

## Parameters / Member Variables
- `rmin`: Lower bound of the random integer range (inclusive, 32-bit signed integer)
- `rmax`: Upper bound of the random integer range (inclusive, 32-bit signed integer)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INT32` - Extracts 32-bit integer arguments from function call
  - `initialize_prng` - Ensures PRNG is seeded before use
  - `pg_prng_int64_range` - Generates unbiased random integer within specified range
  - `PG_RETURN_INT32` - Returns 32-bit integer from PostgreSQL function
- Called from:
  - No direct callers found in the analyzed codebase (likely called from SQL)

## Notes and Other Information
- This is a PostgreSQL SQL-callable function accessible via SQL commands
- Both bounds are inclusive - the result can equal either rmin or rmax
- Input validation ensures rmin ≤ rmax, preventing invalid range specifications
- Uses 64-bit range generation internally then casts to 32-bit for the final result
- The function handles the mathematical complexities of unbiased range mapping through `pg_prng_int64_range()`
- Error handling uses PostgreSQL's standard mechanism with ERRCODE_INVALID_PARAMETER_VALUE
- Thread-safety depends on the underlying PRNG state management
- Commonly used for generating random test data, sampling, or simulation applications
- Function name follows PostgreSQL naming convention: `int4` refers to 4-byte (32-bit) integers