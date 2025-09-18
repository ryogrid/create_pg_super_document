# sampler_random_init_state

## Location
src/backend/utils/misc/sampling.c: 234 - 240

## Overview
Initializes the random number generator state used by PostgreSQL's sampling algorithms with a given seed value.

## Definition


## Detailed Description
This function serves as a wrapper around PostgreSQL's pseudo-random number generator (PRNG) initialization. It takes a 32-bit seed value and initializes the provided PRNG state structure, which is then used by various sampling algorithms throughout PostgreSQL. The function converts the 32-bit seed to a 64-bit value for the underlying PRNG implementation and ensures consistent random number generation for sampling operations.

## Parameters / Member Variables
- : A 32-bit unsigned integer used as the seed for the random number generator
- : Pointer to a pg_prng_state structure that will hold the initialized random number generator state

## Dependencies
- Functions called/Symbols referenced:
  - pg_prng_seed
  - pg_prng_state (type)
- Called from (representative examples):
  - BlockSampler_Init
  - reservoir_init_selection_state
  - [anl_random_fract](../a/anl_random_fract.md)
  - [anl_init_selection_state](../a/anl_init_selection_state.md)

## Notes and Other Information
This function is part of PostgreSQL's sampling infrastructure and is used by various sampling algorithms including block sampling for table analysis and reservoir sampling. The function provides a consistent interface for initializing random states across different sampling contexts, ensuring reproducible results when the same seed is used.