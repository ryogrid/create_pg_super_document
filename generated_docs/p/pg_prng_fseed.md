# pg_prng_fseed

## Location
[src/common/pg_prng.c:102-113](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/pg_prng.c#L102-L113)

## Overview
The `pg_prng_fseed` function initializes a PostgreSQL pseudo-random number generator state from a floating-point seed value in the range [-1.0, 1.0], providing a convenient interface for applications that work with normalized floating-point seeds.

## Definition
```c
void pg_prng_fseed(pg_prng_state *state, double fseed)
```

## Detailed Description
The `pg_prng_fseed` function provides an alternative initialization interface for PostgreSQL's pseudo-random number generator that accepts floating-point seed values. This is particularly useful for applications that prefer to work with normalized seed values or when interfacing with systems that provide seeds as floating-point numbers.

The function performs the following conversion process:
1. Takes a double-precision floating-point seed value in the range [-1.0, 1.0]
2. Scales the floating-point value to utilize approximately 52 mantissa bits (the precision of IEEE 754 double-precision format)
3. Converts the scaled value to a 64-bit signed integer
4. Casts the signed integer to unsigned and calls `pg_prng_seed` to perform the actual state initialization

The scaling factor `(2^52 - 1)` is chosen to maximize the use of the floating-point precision while ensuring the result fits within the range that can be accurately represented. The sign bit of the floating-point number also contributes to the seed value, allowing the full range [-1.0, 1.0] to be utilized effectively.

## Parameters / Member Variables
- `state`: Pointer to a pg_prng_state structure that will be initialized with the new seed
- `fseed`: Double-precision floating-point seed value in the range [-1.0, 1.0]

## Dependencies
- Functions called/Symbols referenced:
  - [pg_prng_seed](pg_prng_seed.md) (called to perform the actual state initialization with the converted integer seed)
  - [pg_prng_state](pg_prng_state.md) (state structure type)
- Called from (representative examples):
  - [geqo_set_seed](../g/geqo_set_seed.md) (genetic algorithm optimizer seed setting)
  - [setseed](../s/setseed.md) (SQL function for setting random seed)

## Notes and Other Information
- This is a public function (no static modifier), making it available to other parts of PostgreSQL
- The function expects fseed values in the range [-1.0, 1.0]; values outside this range may produce unexpected or suboptimal seed distributions
- The conversion process preserves the sign of the floating-point input, so negative and positive seeds will produce different integer values
- The 52-bit precision assumption aligns with IEEE 754 double-precision format, which is standard on most modern systems
- This function is commonly used by PostgreSQL's SQL interface (setseed function) and optimization algorithms that work with normalized parameters
- Like `pg_prng_seed`, this function ensures the resulting state is not all-zeroes through the underlying initialization process
- The function provides a convenient bridge between floating-point-based interfaces and the integer-based underlying PRNG implementation
- Located in src/common/pg_prng.c, making it available to both frontend and backend code

## Simplified Source

```c
void pg_prng_fseed(pg_prng_state *state, double fseed) {
    // Convert floating-point seed to integer using 52-bit mantissa precision
    // Scale by (2^52 - 1) to maximize use of floating-point precision
    int64 seed = ((double) ((UINT64CONST(1) << 52) - 1)) * fseed;

    // Initialize PRNG state with converted integer seed
    pg_prng_seed(state, (uint64) seed);
}
```