# block_sampling_read_stream_next

## Location
[src/backend/commands/analyze.c:1115-1157](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/analyze.c#L1115-L1157)

## Overview
Read stream callback function that returns the next block number selected by the BlockSampling algorithm for table analysis.

## Definition

```c
static BlockNumber
block_sampling_read_stream_next(ReadStream *stream,
								void *callback_private_data,
								void *per_buffer_data)
```
## Detailed Description
This function serves as a callback for the read stream infrastructure during table analysis, implementing the block sampling strategy used by ANALYZE. It interfaces with the BlockSampler algorithm to determine which blocks should be read next during the sampling process. The function uses BlockSamplerData to maintain the sampling state and provides the selected block numbers to the read stream system for efficient I/O.

The function acts as a bridge between the generic read stream framework and the specific block sampling algorithm used for statistical analysis. It returns the next block number that should be read according to the sampling algorithm, or InvalidBlockNumber when no more blocks need to be sampled.

## Parameters / Member Variables
- : The ReadStream structure managing the I/O operations
- : Pointer to BlockSamplerData containing sampling algorithm state
- : Per-buffer private data (unused in this callback)

## Dependencies
- Functions called/Symbols referenced:
  - : Checks if more blocks remain in the sampling sequence
  - : Returns the next block number from the sampling algorithm
- Called from (representative examples):
  - : Main row sampling function that uses this callback with read streams

## Notes and Other Information
- Part of the read stream callback interface for efficient block-level I/O during analysis
- Returns InvalidBlockNumber to signal the end of the sampling sequence
- The BlockSamplerData maintains the sampling algorithm's internal state including random selection
- Used specifically for the block sampling approach to table analysis, which is more efficient than reading entire tables
- Integrates with PostgreSQL's read stream infrastructure for optimized I/O patterns and prefetching

## Simplified Source

```c
static BlockNumber block_sampling_read_stream_next(ReadStream *stream,
                                                   void *callback_private_data,
                                                   void *per_buffer_data)
{
    // Extract the block sampler state
    BlockSamplerData *bs = callback_private_data;

    // Return next block from sampler, or invalid block if done
    return BlockSampler_HasMore(bs) ? BlockSampler_Next(bs) : InvalidBlockNumber;
}
```