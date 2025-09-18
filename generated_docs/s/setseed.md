# setseed

## Location
src/backend/utils/adt/pseudorandomfuncs.c: 62 - 83

## Overview
Seeds the pseudo-random number generator with a specified floating-point value in the range [-1.0, 1.0].

## Definition
```c
Datum setseed(PG_FUNCTION_ARGS)
```

## Detailed Description
This PostgreSQL function allows manual seeding of the global PRNG state with a user-specified seed value. The function accepts a double-precision floating-point number and converts it to an appropriate seed using `pg_prng_fseed()`. This enables reproducible pseudo-random sequences for testing, debugging, or when deterministic randomness is required.

The function validates that the input seed falls within the acceptable range of [-1.0, 1.0] and is not NaN. If validation fails, it raises a PostgreSQL error with appropriate error codes and messages.

## Parameters / Member Variables
- `seed`: A double-precision floating-point value between -1.0 and 1.0 (inclusive) used to seed the PRNG

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_FLOAT8` - Extracts float8 argument from function call
  - `isnan` - Checks if the seed value is NaN
  - `pg_prng_fseed` - Seeds the PRNG with a floating-point value
  - `PG_RETURN_VOID` - Returns void from PostgreSQL function
- Called from:
  - [assign_random_seed](../a/assign_random_seed.md) - Assignment function for random_seed GUC variable

## Notes and Other Information
- This is a PostgreSQL SQL-callable function that follows the PG function interface conventions
- Input validation ensures seed values are within [-1.0, 1.0] range and not NaN
- The function sets the global `prng_seed_set` flag to true after successful seeding
- Error handling uses PostgreSQL's standard error reporting mechanism with ERRCODE_INVALID_PARAMETER_VALUE
- Can be called directly from SQL as `SELECT setseed(0.5);`
- Used internally by the random_seed GUC (Grand Unified Configuration) parameter system