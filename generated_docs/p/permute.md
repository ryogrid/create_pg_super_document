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
- : The input value to permute (automatically reduced modulo size)
- : The size of the permutation range [0, isize)
- : Random seed for initializing the pseudorandom number generator

## Dependencies
- Functions called/Symbols referenced:
  - pg_prng_state (PRNG state structure)
  - pg_prng_seed (initialize PRNG with seed)
  - [pg_leftmost_one_pos64](pg_leftmost_one_pos64.md) (find leftmost bit position)
  - pg_prng_uint64 (generate random 64-bit values)
  - pg_prng_uint64_range (generate random value in range)
- Called from (representative examples):
  - evalStandardFunc

## Notes and Other Information
- **NOT CRYPTOGRAPHICALLY SECURE** - designed for performance in benchmarking contexts
- Returns 0 for sizes < 2 (nothing to permute)
- Uses 6 transformation rounds empirically chosen for good performance/randomness tradeoff
- Handles power-of-2 masking to work with arbitrary size ranges
- Located in src/bin/pgbench/pgbench.c:1303-1393