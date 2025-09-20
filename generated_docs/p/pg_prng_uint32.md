# pg_prng_uint32

## Location
[src/common/pg_prng.c:227-242](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/pg_prng.c#L227-L242)

## Overview
Generates a random 32-bit unsigned integer uniformly distributed over the full range [0, PG_UINT32_MAX].

## Definition

```c
uint32
pg_prng_uint32(pg_prng_state *state)
```
## Detailed Description
This function selects a random uint32 uniformly from the full range [0, PG_UINT32_MAX]. The implementation uses the upper 32 bits of the 64-bit xoroshiro128** generator output to ensure high-quality randomness. Although xoroshiro128** is not known to have weaknesses in low-order bits, PostgreSQL prefers using the upper bits for additional quality assurance.

## Parameters / Member Variables
- : Pointer to the pseudo-random number generator state structure

## Dependencies
- Functions called/Symbols referenced:
  - xoroshiro128ss (the core PRNG algorithm)
  - pg_prng_state (state structure type)
- Called from (representative examples):
  - [_bt_findinsertloc](../b/_bt_findinsertloc.md) (B-tree insertion)
  - [acquire_sample_rows](../a/acquire_sample_rows.md) (table sampling)
  - [ExecInitSampleScan](../E/ExecInitSampleScan.md) (sample scan initialization)
  - [dsm_create](../d/dsm_create.md) (dynamic shared memory)
  - [CatalogCacheCreateEntry](../C/CatalogCacheCreateEntry.md) (catalog cache)
  - Multiple sampling and random selection functions

## Notes and Other Information
- Uses upper 32 bits (v >> 32) from the 64-bit generator for better quality
- Core building block for many PostgreSQL subsystems requiring randomness
- Extensively used in sampling, caching, and data structure operations
- Part of PostgreSQL's unified PRNG interface for consistent random number generation