# pg_prng_uint64_range

## Location
[src/common/pg_prng.c:144-172](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/pg_prng.c#L144-L172)

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
  - [pg_prng_state](pg_prng_state.md) (struct type)
  - [xoroshiro128ss](../x/xoroshiro128ss.md) (underlying PRNG algorithm)
  - [pg_leftmost_one_pos64](pg_leftmost_one_pos64.md) (bit position utility)
  - likely (branch prediction macro)
- Called from (representative examples):
  - [spgdoinsert](../s/spgdoinsert.md) (SP-GiST index operations)
  - [geqo_randint](../g/geqo_randint.md) (genetic algorithm optimizer)
  - [SetTempTablespaces](../S/SetTempTablespaces.md) (tablespace selection)
  - [array_shuffle_n](../a/array_shuffle_n.md) (array shuffling)
  - [random_var](../r/random_var.md) (numeric random variables)
  - [getrand](../g/getrand.md) (pgbench)
  - [pg_prng_int64_range](pg_prng_int64_range.md) (signed integer ranges)

## Notes and Other Information
- Uses bitmask rejection method to ensure uniform distribution without modulo bias
- Typically converges in at most two iterations on average
- Handles empty ranges gracefully by returning rmin
- The bit shifting optimization makes it efficient for large ranges
- Widely used throughout PostgreSQL for bounded random number generation
- Forms the foundation for other range-based PRNG functions
- Located in `src/common/pg_prng.c` at lines 144-172

## Simplified Source

```c
uint64
pg_prng_uint64_range(pg_prng_state *state, uint64 rmin, uint64 rmax)
{
    uint64 val;

    if (rmax > rmin) {
        // Calculate range size and determine bit shift amount
        uint64 range = rmax - rmin;
        uint32 rshift = 63 - pg_leftmost_one_pos64(range);

        // Use rejection method to ensure uniform distribution
        do {
            val = xoroshiro128ss(state) >> rshift;
        } while (val > range);
    } else {
        val = 0;  // Empty range case
    }

    return rmin + val;
}
```