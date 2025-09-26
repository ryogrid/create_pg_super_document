# BlockSampler_Init

## Location
[src/backend/utils/misc/sampling.c:39-57](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/sampling.c#L39-L57)

## Overview
Prepares a BlockSampler for random sampling of block numbers from a relation, implementing Algorithm S from Knuth 3.4.2 for block-level sampling.

## Definition

```c
BlockNumber
BlockSampler_Init(BlockSampler bs, BlockNumber nblocks, int samplesize,
				  uint32 randseed)
```
## Detailed Description
BlockSampler_Init initializes a BlockSampler structure to perform random sampling of blocks from a PostgreSQL relation. This function implements the foundational setup for block-level sampling as discussed in pgsql-hackers 2004-04-02 (subject "Large DB"). 

The algorithm selects a random sample of  blocks out of the total  blocks in the table. If the table has fewer than  blocks, all blocks are selected. Since the total number of blocks is known in advance, it uses the straightforward Algorithm S from Knuth 3.4.2, rather than more complex algorithms like Vitter's.

The function initializes all necessary state variables and sets up the random number generator with the provided seed. It returns the actual number of blocks that will be sampled, which is the minimum of the requested sample size and the total number of blocks available.

## Parameters / Member Variables
- : Pointer to the BlockSampler structure to initialize
- : Total number of blocks in the relation (measured table size)
- : Desired number of blocks to sample
- : Seed for the random number generator to ensure reproducible sampling

## Dependencies
- Functions called/Symbols referenced:
  - sampler_random_init_state (initializes the random number generator state)
  - Min (macro to find minimum value)
  - BlockSamplerData structure members (N, n, t, m, randstate)
- Called from (representative examples):
  - acquire_sample_rows (in src/backend/commands/analyze.c:1187)

## Notes and Other Information
- The function implements Knuth's Algorithm S, which is simpler than Vitter's algorithm since the total population size is known
- The sampling state includes counters for blocks scanned (t) and blocks selected (m), both initialized to 0
- Returns the effective sample size, which may be smaller than requested if the table has fewer blocks
- The random seed parameter allows for reproducible sampling results across multiple runs
- This is part of PostgreSQL's ANALYZE command infrastructure for collecting table statistics