# BlockSampler_Next

## Location
[src/backend/utils/misc/sampling.c:64-132](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/sampling.c#L64-L132)

## Overview
Returns the next block number to be sampled using Knuth's Algorithm S, implementing an optimized version that reduces random number generation calls.

## Definition

```c
BlockNumber
BlockSampler_Next(BlockSampler bs)
```
## Detailed Description
BlockSampler_Next implements the core logic of Knuth's Algorithm S for block sampling, but with an important optimization. The standard algorithm would require one random number generation per block examined, but this implementation reduces it to one random call per selected block.

The function calculates the probability of skipping the current block (1 - k/K, where k is remaining blocks to sample and K is remaining total blocks). If all remaining blocks are needed (k >= K), it selects the current block immediately. Otherwise, it uses an optimized probabilistic approach where a single random value V is reused across multiple iterations by adjusting the probability threshold.

The algorithm maintains the invariant that K = N - t (remaining blocks equals total blocks minus those examined) and ensures that exactly the desired number of blocks will be sampled without the possibility of running short.

## Parameters / Member Variables
- : Pointer to the BlockSampler structure containing the current sampling state

## Dependencies
- Functions called/Symbols referenced:
  - BlockSampler_HasMore (verifies sampling should continue)
  - sampler_random_fract (generates random fraction in (0,1))
  - Assert (validates preconditions)
  - BlockSamplerData structure members (N, t, n, m, randstate)
- Called from (representative examples):
  - block_sampling_read_stream_next (in src/backend/commands/analyze.c:1121)

## Notes and Other Information
- Implements an optimized version of Knuth's Algorithm S that reduces random number generation overhead
- The optimization reinterprets the random value V across multiple probabilistic tests by adjusting the probability threshold p
- Uses "<" instead of "<=" in the loop condition to avoid potential roundoff errors
- Guarantees that exactly the desired number of blocks will be selected (cannot fail due to the K >= k invariant)
- The algorithm handles the edge case where all remaining blocks must be selected (k >= K) efficiently
- Returns the current block number (bs->t) before incrementing it, following the convention of returning the block being selected
- Must only be called when BlockSampler_HasMore returns true to avoid assertion failures