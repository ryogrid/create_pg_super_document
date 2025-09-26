# pg_prng_uint64_range

## Location
src/common/pg_prng.c: 144 - 172

## Overview
Generates a random 64-bit unsigned integer uniformly distributed within a specified range [rmin, rmax].

## Definition
```c
uint64 pg_prng_uint64_range(pg_prng_state *state, uint64 rmin, uint64 rmax)
```

## Detailed Description
This function generates random numbers within a specific range using an efficient bitmask rejection method. The algorithm ensures uniform distribution by avoiding modulo bias that would occur with simple modular arithmetic. It uses bit shifting to generate candidate values and rejects those that fall outside the desired range.

The implementation employs a sophisticated approach: it calculates the range size, determines the appropriate right-shift amount based on the position of the leftmost bit in the range, and then generates candidates by shifting the output of the underlying PRNG. This method typically requires at most two iterations on average, making it both efficient and mathematically sound.

When the range is empty (rmax <= rmin), the function returns rmin, providing predictable behavior for edge cases.

## Parameters / Member Variables
- `state`: Pointer to a `pg_prng_state` structure that maintains the PRNG state
- `rmin`: Minimum value of the desired range (inclusive)
- `rmax`: Maximum value of the desired range (inclusive)

## Dependencies
- Functions called/Symbols referenced:
  - pg_prng_state (struct type)
  - xoroshiro128ss (underlying PRNG algorithm)
  - pg_leftmost_one_pos64 (bit position utility)
  - likely (branch prediction macro)
- Called from (representative examples):
  - spgdoinsert (SP-GiST index operations)
  - geqo_randint (genetic algorithm optimizer)
  - SetTempTablespaces (tablespace selection)
  - array_shuffle_n (array shuffling)
  - random_var (numeric random variables)
  - getrand (pgbench)
  - pg_prng_int64_range (signed integer ranges)

## Notes and Other Information
- Uses bitmask rejection method to ensure uniform distribution without modulo bias
- Typically converges in at most two iterations on average
- Handles empty ranges gracefully by returning rmin
- The bit shifting optimization makes it efficient for large ranges
- Widely used throughout PostgreSQL for bounded random number generation
- Forms the foundation for other range-based PRNG functions
- Located in `src/common/pg_prng.c` at lines 144-172