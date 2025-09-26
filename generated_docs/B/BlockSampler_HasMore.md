# BlockSampler_HasMore

## Location
src/backend/utils/misc/sampling.c: 58 - 63

## Overview
Checks whether the BlockSampler has more blocks to sample, determining if the sampling process should continue.

## Definition

```c
bool
BlockSampler_HasMore(BlockSampler bs)
```
## Detailed Description
BlockSampler_HasMore is a predicate function that determines whether the block sampling process should continue. It implements the continuation condition for Knuth's Algorithm S by checking two criteria: whether there are more blocks to examine in the table (t < N) and whether the desired sample size has not yet been reached (m < n).

The function returns true only when both conditions are met, ensuring the sampling algorithm continues until either all blocks have been examined or the required number of sample blocks has been collected, whichever comes first.

## Parameters / Member Variables
- : Pointer to the BlockSampler structure containing the sampling state

## Dependencies
- Functions called/Symbols referenced:
  - BlockSamplerData structure members (t, N, m, n)
- Called from (representative examples):
  - block_sampling_read_stream_next (in src/backend/commands/analyze.c:1121)
  - BlockSampler_Next (in src/backend/utils/misc/sampling.c:71)

## Notes and Other Information
- This function is essential for the main sampling loop in Algorithm S implementation
- The two conditions ensure optimal termination: stop when either the sample is complete (m >= n) or the entire table has been scanned (t >= N)
- Used internally by BlockSampler_Next to determine when to continue the sampling process
- Always called before BlockSampler_Next to avoid unnecessary computation when sampling is complete
- Return value of false indicates the sampling process is finished and no more blocks will be selected