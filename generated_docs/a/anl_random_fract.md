# anl_random_fract

## Location
src/backend/utils/misc/sampling.c: 266 - 280

## Overview
Provides a convenient interface for generating random fractions for ANALYZE operations, managing its own global random state with lazy initialization.

## Definition
```c
double anl_random_fract(void)
```

## Detailed Description
This function serves as a simplified wrapper around the sampling random number generation system specifically for ANALYZE operations. It maintains a global random state (oldrs) that is initialized lazily on first use. The initialization uses a seed derived from PostgreSQL's global PRNG state, ensuring different random sequences across different database sessions or restarts. Once initialized, it delegates to sampler_random_fract() to generate uniformly distributed random values in the range (0, 1).

## Parameters / Member Variables
This function takes no parameters and manages its own internal state.

## Dependencies
- Functions called/Symbols referenced:
  - sampler_random_init_state
  - pg_prng_uint32
  - sampler_random_fract
- Called from (representative examples):
  - Referenced in MAX_STATISTICS_TARGET context
  - Used in ReservoirState context

## Notes and Other Information
The function uses a global static variable `oldrs_initialized` to track whether the random state has been set up. This lazy initialization pattern ensures that the random state is only created when needed and avoids unnecessary initialization overhead. The use of the unlikely() macro around the initialization check optimizes for the common case where the state is already initialized. This function is part of PostgreSQL's table analysis infrastructure and provides a convenient way for ANALYZE operations to access random numbers without managing their own random state.