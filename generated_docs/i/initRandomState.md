# initRandomState

## Location
[src/bin/pgbench/pgbench.c:1088-1101](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L1088-L1101)

## Overview
Initializes a pseudo-random number generator (PRNG) state structure by seeding it with a value derived from the global base random sequence.

## Definition

```c
static void
initRandomState(pg_prng_state *state)
```
## Detailed Description
The  function provides a standardized way to initialize PostgreSQL's pseudo-random number generator state structures within pgbench. It seeds the provided PRNG state using a 64-bit random value obtained from the global . This approach ensures that each initialized PRNG state gets a unique, unpredictable seed while maintaining deterministic behavior when the base sequence is seeded consistently.

The function serves as a wrapper around PostgreSQL's PRNG seeding mechanism, providing a clean interface for pgbench components that need their own independent random number streams while ensuring all streams derive from a common, controllable source.

## Parameters / Member Variables
- `*state`: Pointer to a  structure that will be initialized with a new random seed
## Dependencies
- Functions called/Symbols referenced:
  - [pg_prng_state](../p/pg_prng_state.md) (PostgreSQL PRNG state type)
  - [pg_prng_seed](../p/pg_prng_seed.md) (PostgreSQL PRNG seeding function)
  - [pg_prng_uint64](../p/pg_prng_uint64.md) (PostgreSQL PRNG 64-bit value generator)
  - base_random_sequence (global PRNG state used as entropy source)
- Called from (representative examples):
  - [main](../m/main.md) (at src/bin/pgbench/pgbench.c:7237, 7329, 7330, 7331)

## Notes and Other Information
- Function is declared static, limiting its scope to the pgbench.c file
- Requires that  be properly initialized before use
- Part of pgbench's random number generation infrastructure for performance testing
- Multiple calls to this function will produce different seeds, ensuring independent random streams
- Critical for maintaining reproducible benchmark results when base sequence is seeded consistently
- Used during pgbench initialization to set up separate random number generators for different benchmark operations

## Simplified Source

```c
static void initRandomState(pg_prng_state *state) {
    // Seed the PRNG state with a 64-bit random value from the global base sequence
    pg_prng_seed(state, pg_prng_uint64(&base_random_sequence));
}
```

**Key Points:**
- Simple wrapper around PostgreSQL's PRNG seeding
- Derives seed from global base_random_sequence for reproducible benchmarks
- Each call produces a different seed for independent random streams