# bernoulli_nextsampletuple

## Location
src/backend/access/tablesample/bernoulli.c: 181 - 229

## Overview
This function selects the next sampled tuple in the current block by performing probabilistic sampling decisions for each tuple offset using hash-based random number generation.

## Definition
```c
static OffsetNumber bernoulli_nextsampletuple(SampleScanState *node,
                                              BlockNumber blockno,
                                              OffsetNumber maxoffset)
```

## Detailed Description
The `bernoulli_nextsampletuple` function implements the core logic of Bernoulli sampling by examining each tuple offset in a block and making probabilistic inclusion decisions. It uses a deterministic hash function combining the block number, tuple offset, and random seed to generate consistent random values for each tuple. These hash values are compared against the pre-calculated cutoff threshold to determine tuple selection. The function processes tuple offsets sequentially until it finds one that should be sampled or reaches the end of the block. Importantly, it performs sampling decisions regardless of tuple visibility, as the probabilistic nature ensures fair sampling even when some tuples are invisible.

## Parameters / Member Variables
- `node`: SampleScanState structure representing the sample scan execution node
- `blockno`: Block number of the current block being scanned
- `maxoffset`: Maximum valid tuple offset in the current block
- Returns: OffsetNumber of the next selected tuple, or InvalidOffsetNumber if no more tuples in block

## Dependencies
- Functions called/Symbols referenced:
  - [hash_any](../h/hash_any.md) (generates hash from input array for random number generation)
  - [DatumGetUInt32](../D/DatumGetUInt32.md) (extracts uint32 hash value from Datum)
  - FirstOffsetNumber (constant for first valid tuple offset)
  - InvalidOffsetNumber (constant indicating no valid tuple)
- Called from (representative examples):
  - [tsm_bernoulli_handler](../t/tsm_bernoulli_handler.md) (sets this as NextSampleTuple callback)

## Notes and Other Information
- Uses hash_any with 3 uint32 inputs: block number, tuple offset, and seed for machine-independent results
- Performs "coinflip" for every tuple offset, ensuring equal probability regardless of tuple visibility
- Continues from the last selected tuple offset (stored in sampler->lt) to maintain scan state
- Hash-based approach provides repeatable results when using the same seed
- Returns InvalidOffsetNumber when reaching end of block to signal block completion
- The sampling decision is made by comparing hash < cutoff, where cutoff was calculated during scan initialization
- This is a static function, only callable within the bernoulli.c module
- The algorithm ensures that invisible or non-existent tuples don't bias the sampling distribution