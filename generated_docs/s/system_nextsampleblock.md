# system_nextsampleblock

## Location
src/backend/access/tablesample/system.c: 178 - 235

## Overview
Selects the next block to sample by iterating through blocks and using hash-based probabilistic selection to determine which blocks should be included in the sample.

## Definition
```c
static BlockNumber system_nextsampleblock(SampleScanState *node, BlockNumber nblocks)
```

## Detailed Description
This function implements the core block selection algorithm for the SYSTEM table sampling method. It uses a deterministic hash-based approach to decide which blocks to include in the sample. For each block, it computes a hash value using the block number and the random seed, then compares this hash against a pre-calculated cutoff value to make the sampling decision. This approach ensures that the same blocks are consistently selected for a given seed and sampling percentage, enabling reproducible results. The function maintains state to efficiently continue from where it left off on subsequent calls, avoiding the need to re-examine previously considered blocks.

## Parameters / Member Variables
- `node`: SampleScanState structure containing the sample scan execution state and sampler data
- `nblocks`: Total number of blocks in the relation being sampled

## Dependencies
- Functions called/Symbols referenced:
  - hash_any (computes hash value from byte array)
  - DatumGetUInt32 (extracts uint32 value from hash result)
  - InvalidBlockNumber (constant representing invalid block)
- Called from (representative examples):
  - tsm_system_handler (as function pointer in TsmRoutine)
  - PostgreSQL executor during sample scan execution

## Notes and Other Information
- Uses a 2-element uint32 array as hash input: block number and seed value
- The hash_any function provides machine-independent results, which is important for consistent behavior across different platforms
- Maintains nextblock state to resume scanning from the correct position on subsequent calls
- Returns InvalidBlockNumber when no more suitable blocks are found
- The hash comparison (hash < cutoff) implements the probabilistic sampling decision
- Resets nextblock to 0 when reaching the end of relation for safety, though this should rarely matter in practice
- The deterministic nature of the hash function ensures repeatable sampling when the same seed is used