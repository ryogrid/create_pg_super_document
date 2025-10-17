# anl_random_fract

## Location
[src/backend/utils/misc/sampling.c:266-280](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/sampling.c#L266-L280)

## Overview
Provides a convenient interface for generating random fractions for ANALYZE operations, managing its own global random state with lazy initialization.

## Definition
```c
double anl_random_fract(void)
```

## Detailed Description
This function serves as a simplified wrapper around the sampling random number generation system specifically for ANALYZE operations. It maintains a global random state (oldrs) that is initialized lazily on first use. The initialization uses a seed derived from PostgreSQL's global PRNG state, ensuring different random sequences across different database sessions or restarts. Once initialized, it delegates to sampler_random_fract() to generate uniformly distributed random values in the range (0, 1).

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [sampler_random_init_state](../s/sampler_random_init_state.md)
  - [pg_prng_uint32](../p/pg_prng_uint32.md)
  - [sampler_random_fract](../s/sampler_random_fract.md)
- Called from (representative examples):
  - Referenced in MAX_STATISTICS_TARGET context
  - Used in ReservoirState context

## Notes and Other Information
The function uses a global static variable `oldrs_initialized` to track whether the random state has been set up. This lazy initialization pattern ensures that the random state is only created when needed and avoids unnecessary initialization overhead. The use of the unlikely() macro around the initialization check optimizes for the common case where the state is already initialized. This function is part of PostgreSQL's table analysis infrastructure and provides a convenient way for ANALYZE operations to access random numbers without managing their own random state.

## Simplified Source

```c
double anl_random_fract(void) {
    // Initialize random state on first use
    if (unlikely(!oldrs_initialized)) {
        sampler_random_init_state(pg_prng_uint32(&pg_global_prng_state),
                                  &oldrs.randstate);
        oldrs_initialized = true;
    }

    // Generate and return a random fraction
    return sampler_random_fract(&oldrs.randstate);
}
```