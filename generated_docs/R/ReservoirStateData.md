# ReservoirStateData

## Location
[src/include/utils/sampling.h:50-51](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/sampling.h#L50-L51)

## Overview
ReservoirStateData is a data structure that maintains the state for reservoir sampling algorithms, used in PostgreSQL for selecting random samples when the population size is unknown or very large.

## Definition
```c
typedef struct
{
    double          W;
    pg_prng_state randstate;    /* random generator state */
} ReservoirStateData;
```

## Detailed Description
ReservoirStateData implements the state management for reservoir sampling algorithms, which are used when the total population size is not known in advance or when dealing with very large datasets. Unlike BlockSamplerData which uses Knuth's Algorithm S for known population sizes, reservoir sampling maintains a fixed-size sample by probabilistically replacing previously selected items as new items are encountered.

The W field represents a key parameter in the reservoir sampling algorithm that helps determine skip distances and selection probabilities. This approach is particularly useful for sampling from data streams or very large tables where scanning the entire dataset to determine its size would be prohibitively expensive.

## Parameters / Member Variables
- `W`: A statistical parameter used in the reservoir sampling algorithm to calculate skip distances and selection probabilities
- `randstate`: The pseudo-random number generator state used for making sampling decisions

## Dependencies
- Functions called/Symbols referenced:
  - [pg_prng_state](../p/pg_prng_state.md) (for random number generation)
  - double (for statistical calculations)
- Called from (representative examples):
  - [acquire_sample_rows](../a/acquire_sample_rows.md) (src/backend/commands/analyze.c:1171)
  - [sampler_random_fract](../s/sampler_random_fract.md) (src/backend/utils/misc/sampling.c:262)

## Notes and Other Information
- Used as the basis for the ReservoirState pointer type (typedef ReservoirStateData *ReservoirState)
- Part of PostgreSQL's reservoir sampling implementation for cases where population size is unknown
- Complementary to BlockSamplerData - used when the block-based sampling approach is not suitable
- Associated with functions like reservoir_init_selection_state() and reservoir_get_next_S()
- Provides more flexible sampling capabilities compared to block sampling for streaming or very large data scenarios
- Located in src/include/utils/sampling.h:50-51