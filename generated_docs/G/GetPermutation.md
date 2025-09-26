# GetPermutation

## Location
[src/test/modules/test_rbtree/test_rbtree.c:94-126](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_rbtree/test_rbtree.c#L94-L126)

## Overview
Generates a random permutation of integers from 0 to size-1 using the Fisher-Yates shuffle algorithm for use in red-black tree testing scenarios.

## Definition

```c
static int *
GetPermutation(int size)
```
## Detailed Description
GetPermutation implements the "inside-out" variant of the Fisher-Yates shuffle algorithm to generate a uniformly random permutation of consecutive integers starting from 0. This function is specifically designed for testing red-black tree operations by providing randomized insertion orders.

The algorithm works by iteratively building the permutation array. For each position i from 1 to size-1, it selects a random index j from 0 to i (inclusive), then places the current value i at position j while moving the previous value at position j to position i. This approach efficiently combines the insertion and swapping steps of the traditional Fisher-Yates algorithm.

The function ensures that all possible permutations have equal probability of being generated, making it suitable for comprehensive testing scenarios where insertion order randomness is crucial for exercising different tree structures.

## Parameters / Member Variables
- `size`: The number of integers in the desired permutation (must be positive); generates integers 0 through size-1

## Dependencies
- Functions called/Symbols referenced:
  - palloc (PostgreSQL memory allocation)
  - pg_prng_uint64_range (PostgreSQL pseudorandom number generator)
  - pg_global_prng_state (global PRNG state)
- Called from (representative examples):
  - rbt_populate (in test_rbtree.c:129)

## Notes and Other Information
- The function uses PostgreSQL's memory management (palloc) rather than standard malloc
- Relies on PostgreSQL's built-in PRNG for reproducible test results
- The "inside-out" variant is more cache-friendly than the traditional Fisher-Yates shuffle
- Critical for red-black tree testing as it ensures diverse tree structures through randomized insertion patterns
- Part of the test_rbtree module used for validating red-black tree correctness