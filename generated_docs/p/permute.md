# permute

## Location
[src/bin/pgbench/pgbench.c:1303-1393](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L1303-L1393)

## Overview
Generates pseudorandom permutations of integers in the range [0, size) for pgbench workload generation, providing roughly uniform distribution across all possible permutations for small sizes.

## Definition

```c
static int64
permute(const int64 val, const int64 isize, const int64 seed)
```
## Detailed Description
This function implements a pseudorandom permutation algorithm designed for pgbench's workload generation needs. For small sizes (≤20), it can generate each of the (size!) possible permutations with roughly equal probability. For larger sizes, while not all permutations are possible due to the finite state space of the PRNG, it still provides good random distribution.

The algorithm uses a modified linear congruential generator approach with six rounds of bijective transformations. Each round applies:
1. Random multiplication by odd numbers on overlapping upper/lower halves of the input
2. XOR operations with random values  
3. Bit rotations to improve randomness distribution
4. Random offsets modulo the full range

This approach separates adjacent inputs and distributes values uniformly across the output range, creating effective pseudorandom permutations suitable for database benchmarking scenarios.

## Parameters / Member Variables
- `val`: The input value to permute (automatically reduced modulo size)
- `isize`: The size of the permutation range [0, isize)
- `seed`: Random seed for initializing the pseudorandom number generator
## Dependencies
- Functions called/Symbols referenced:
  - [pg_prng_state](pg_prng_state.md) (PRNG state structure)
  - [pg_prng_seed](pg_prng_seed.md) (initialize PRNG with seed)
  - [pg_leftmost_one_pos64](pg_leftmost_one_pos64.md) (find leftmost bit position)
  - [pg_prng_uint64](pg_prng_uint64.md) (generate random 64-bit values)
  - [pg_prng_uint64_range](pg_prng_uint64_range.md) (generate random value in range)
- Called from (representative examples):
  - [evalStandardFunc](../e/evalStandardFunc.md)

## Notes and Other Information
- **NOT CRYPTOGRAPHICALLY SECURE** - designed for performance in benchmarking contexts
- Returns 0 for sizes < 2 (nothing to permute)
- Uses 6 transformation rounds empirically chosen for good performance/randomness tradeoff
- Handles power-of-2 masking to work with arbitrary size ranges
- Located in src/bin/pgbench/pgbench.c:1303-1393

## Simplified Source

```c
static int64 permute(const int64 val, const int64 isize, const int64 seed) {
    // Pseudorandom permutation for pgbench workload generation
    // NOT cryptographically secure - designed for benchmarking performance

    if (isize < 2) return 0;  // Nothing to permute

    // Initialize PRNG with seed
    pg_prng_state state;
    pg_prng_seed(&state, (uint64) seed);

    // Work with unsigned values
    uint64 size = (uint64) isize;
    uint64 v = (uint64) val % size;

    // Calculate mask for largest power of 2 <= size
    int masklen = pg_leftmost_one_pos64(size);
    uint64 mask = (((uint64) 1) << masklen) - 1;

    // Apply 6 rounds of bijective transformations
    for (int i = 0; i < 6; i++) {
        // Transform lower half: multiply (odd), XOR, rotate
        uint64 m = (pg_prng_uint64(&state) & mask) | 1;  // Ensure odd
        uint64 r = pg_prng_uint64(&state) & mask;
        if (v <= mask) {
            v = ((v * m) ^ r) & mask;
            v = ((v << 1) & mask) | (v >> (masklen - 1));  // Rotate
        }

        // Transform upper half: same operations on (size-1-v)
        m = (pg_prng_uint64(&state) & mask) | 1;
        r = pg_prng_uint64(&state) & mask;
        uint64 t = size - 1 - v;
        if (t <= mask) {
            t = ((t * m) ^ r) & mask;
            t = ((t << 1) & mask) | (t >> (masklen - 1));
            v = size - 1 - t;
        }

        // Apply random offset across full range
        r = pg_prng_uint64_range(&state, 0, size - 1);
        v = (v + r) % size;
    }

    return (int64) v;
}
```

**Key Points:**
- Generates pseudorandom permutations for values in range [0, size)
- Uses 6 rounds of bijective transformations (multiply, XOR, rotate, offset)
- For small sizes (≤20), achieves roughly uniform distribution over all permutations
- Essential for creating realistic data access patterns in benchmarks