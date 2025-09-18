# drandom_normal

## Location
[src/backend/utils/adt/pseudorandomfuncs.c:102-125](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pseudorandomfuncs.c#L102-L125)

## Overview
Returns a pseudo-random floating-point number from a normal (Gaussian) distribution with specified mean and standard deviation parameters.

## Definition
```c
Datum drandom_normal(PG_FUNCTION_ARGS)
```

## Detailed Description
This PostgreSQL function generates pseudo-random double-precision floating-point numbers following a normal (Gaussian) distribution with user-specified mean and standard deviation. The function uses a two-step process:

1. First, it generates a standard normal variable (mean=0, stddev=1) using `pg_prng_double_normal()`
2. Then transforms this standard normal variable to match the target distribution using the linear transformation: result = (stddev * z) + mean

This transformation preserves the normal distribution properties while scaling and shifting to achieve the desired mean and standard deviation. The function ensures proper PRNG initialization before generating values.

## Parameters / Member Variables
- `mean`: The desired mean (center) of the normal distribution (double-precision float)
- `stddev`: The desired standard deviation (spread) of the normal distribution (double-precision float)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_FLOAT8` - Extracts double-precision arguments (mean and stddev)
  - [initialize_prng](../i/initialize_prng.md) - Ensures PRNG is seeded before use
  - [pg_prng_double_normal](../p/pg_prng_double_normal.md) - Generates standard normal distributed value
- Called from:
  - No direct callers found in the analyzed codebase (likely called from SQL)

## Notes and Other Information
- This is a PostgreSQL SQL-callable function accessible via SQL commands
- Implements the standard Box-Muller or similar algorithm for normal distribution generation
- The transformation formula (stddev * z + mean) is mathematically correct for normal distributions
- No validation is performed on input parameters - negative standard deviation could produce unexpected results
- Thread-safety depends on the underlying PRNG state management
- Commonly used for statistical simulations, data analysis, and generating realistic test data
- The function name suggests 'double random normal' to indicate double-precision normal distribution