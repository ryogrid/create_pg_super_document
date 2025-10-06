# drandom

## Location
[src/backend/utils/adt/pseudorandomfuncs.c:84-101](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pseudorandomfuncs.c#L84-L101)

## Overview
Returns a pseudo-random floating-point number uniformly distributed in the range [0.0, 1.0).

## Definition
```c
Datum drandom(PG_FUNCTION_ARGS)
```

## Detailed Description
This PostgreSQL function generates pseudo-random double-precision floating-point numbers with uniform distribution across the half-open interval [0.0, 1.0). The function ensures the PRNG is properly initialized by calling `initialize_prng()` before generating values, guaranteeing consistent behavior regardless of when it's first called in a process.

The function leverages PostgreSQL's internal `pg_prng_double()` function which produces values in the exact desired range. This makes it suitable for applications requiring uniform random distribution, such as statistical sampling, random selections, or Monte Carlo simulations.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [initialize_prng](../i/initialize_prng.md) - Ensures PRNG is seeded before use
  - [pg_prng_double](../p/pg_prng_double.md) - Generates uniform double in [0.0, 1.0) range
- Called from:
  - No direct callers found in the analyzed codebase (likely called from SQL)

## Notes and Other Information
- This is a PostgreSQL SQL-callable function accessible via `SELECT random();` SQL command
- The function always returns values in the half-open interval [0.0, 1.0) - includes 0.0 but excludes 1.0
- Thread-safety depends on the underlying PRNG state management
- Initialization is lazy - PRNG is seeded only when first needed
- The function name `drandom` likely stands for 'double random' to distinguish from integer random functions
- Part of PostgreSQL's standard mathematical function library

## Simplified Source

```c
Datum
drandom(PG_FUNCTION_ARGS)
{
    // Ensure PRNG is initialized
    initialize_prng();

    // Generate uniform random double in [0.0, 1.0)
    float8 result = pg_prng_double(&prng_state);

    PG_RETURN_FLOAT8(result);
}
```