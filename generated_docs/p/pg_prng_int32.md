# pg_prng_int32

## Location
[src/common/pg_prng.c:243-253](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/pg_prng.c#L243-L253)

## Overview
Generates a random 32-bit signed integer uniformly distributed over the full range [PG_INT32_MIN, PG_INT32_MAX].

## Definition

```c
int32
pg_prng_int32(pg_prng_state *state)
```
## Detailed Description
This function selects a random int32 uniformly from the full signed 32-bit integer range [PG_INT32_MIN, PG_INT32_MAX]. Like pg_prng_uint32, it uses the upper 32 bits of the 64-bit xoroshiro128** generator output, then casts the result to a signed integer. This provides uniform coverage of both positive and negative 32-bit integers.

## Parameters / Member Variables
- : Pointer to the pseudo-random number generator state structure

## Dependencies
- Functions called/Symbols referenced:
  - [xoroshiro128ss](../x/xoroshiro128ss.md) (the core PRNG algorithm)
  - [pg_prng_state](pg_prng_state.md) (state structure type)
- Called from (representative examples):
  - [prepare_buf](prepare_buf.md) (in pg_test_fsync utility)

## Notes and Other Information
- Uses upper 32 bits (v >> 32) from the 64-bit generator, same as pg_prng_uint32
- Simple cast from uint32 to int32 provides uniform distribution over signed range
- Less commonly used compared to pg_prng_uint32 and pg_prng_int32p
- Part of PostgreSQL's unified PRNG interface for consistent random number generation
- Provides full signed 32-bit range including negative values