# BlockSamplerData

## Location
[src/include/utils/sampling.h:35-36](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/sampling.h#L35-L36)

## Overview
BlockSamplerData is a data structure that implements Algorithm S from Knuth 3.4.2 for efficiently sampling blocks from a relation without replacement in PostgreSQL's statistics collection system.

## Definition

```c
typedef BlockSamplerData *BlockSampler;
```
## Detailed Description
BlockSamplerData implements Knuth's Algorithm S for reservoir sampling of blocks from a PostgreSQL relation. This algorithm is used primarily during table analysis (ANALYZE command) to select a representative sample of blocks for statistical purposes. The algorithm ensures that each block has an equal probability of being selected while processing the blocks sequentially, making it efficient for large tables where the total number of blocks is known in advance.

The structure maintains the state needed to determine probabilistically whether each encountered block should be included in the sample, ensuring uniform random sampling without the need to store all possible blocks in memory.

## Parameters / Member Variables
- : The total number of blocks in the relation, known before sampling begins
- : The desired number of blocks to sample (target sample size)
- : The current block number being considered during the sampling process
- : The number of blocks that have been selected for the sample so far
- : The pseudo-random number generator state used for sampling decisions

## Dependencies
- Functions called/Symbols referenced:
  - [pg_prng_state](../p/pg_prng_state.md) (for random number generation)
  - BlockNumber (PostgreSQL block identifier type)
- Called from (representative examples):
  - [block_sampling_read_stream_next](../b/block_sampling_read_stream_next.md) (src/backend/commands/analyze.c:1119)
  - [acquire_sample_rows](../a/acquire_sample_rows.md) (src/backend/commands/analyze.c:1170)

## Notes and Other Information
- Used as the basis for the BlockSampler pointer type (typedef BlockSamplerData *BlockSampler)
- Implements Knuth's Algorithm S which provides optimal performance for cases where the population size (N) is known in advance
- Critical component of PostgreSQL's table analysis system for generating table statistics
- The algorithm maintains statistical properties ensuring each block has equal probability N/n of being selected
- Located in src/include/utils/sampling.h:35-36