# numeric_random

## Location
[src/backend/utils/adt/pseudorandomfuncs.c:174-185](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pseudorandomfuncs.c#L174-L185)

## Overview
Generates a random numeric value uniformly distributed within a specified range.

## Definition

```c
Datum
numeric_random(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in function that generates pseudo-random numeric values within a user-specified range. It accepts two Numeric parameters defining the lower and upper bounds and returns a uniformly distributed random Numeric value between these bounds. Unlike  which works with integer types, this function operates on PostgreSQL's arbitrary-precision numeric type, allowing for decimal precision and very large number ranges. The function uses PostgreSQL's internal pseudo-random number generator which is initialized on first use.

## Parameters / Member Variables
-  (Numeric): The lower bound (inclusive) for the random number generation
-  (Numeric): The upper bound (inclusive) for the random number generation

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NUMERIC: Extracts Numeric arguments from PostgreSQL function call
  - Numeric: PostgreSQL's arbitrary-precision numeric data type
  - [initialize_prng](../i/initialize_prng.md): Initializes the pseudo-random number generator state
  - [random_numeric](../r/random_numeric.md): Generates a random Numeric within the specified range
  - PG_RETURN_NUMERIC: Returns a Numeric value from PostgreSQL function
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- Works with PostgreSQL's Numeric type, supporting arbitrary precision and scale
- Uses the same PRNG state as other random functions for consistency
- Part of PostgreSQL's pseudo-random functions subsystem located in pseudorandomfuncs.c
- The function is designed to be called from SQL as a built-in function
- Supports decimal numbers and very large numeric ranges unlike integer-based random functions