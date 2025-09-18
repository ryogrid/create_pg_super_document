# BlockIdSet

## Location
src/include/storage/block.h: 81 - 91

## Overview
BlockIdSet is an inline function that sets a block identifier to a specified block number value by splitting a 32-bit block number into two 16-bit components.

## Definition
```c
static inline void BlockIdSet(BlockIdData *blockId, BlockNumber blockNumber)
```

## Detailed Description
BlockIdSet initializes a BlockIdData structure by decomposing a BlockNumber (32-bit unsigned integer) into its high and low 16-bit components. The function splits the block number using bit shifting and masking operations to store it in the bi_hi and bi_lo fields of the BlockIdData structure. This encoding allows block numbers to be stored efficiently in a format that can be SHORTALIGN'd, which is important for reducing space requirements in on-disk structures like heap tuples and line pointer arrays.

## Parameters / Member Variables
- `blockId`: Pointer to the BlockIdData structure to be initialized
- `blockNumber`: The BlockNumber (32-bit value) to be stored in the block identifier

## Dependencies
- Functions called/Symbols referenced:
  - BlockIdData (structure type)
- Called from (representative examples):
  - ginPlaceToPage
  - PostingItemSetBlockNumber
  - ItemPointerSet
  - ItemPointerSetBlockNumber
  - ItemPointerSetInvalid

## Notes and Other Information
- This is a static inline function defined in src/include/storage/block.h for optimal performance
- The encoding splits a 32-bit BlockNumber into: bi_hi = blockNumber >> 16, bi_lo = blockNumber & 0xffff
- Part of PostgreSQL's block management system for efficient storage space utilization
- BlockIdData structures can be SHORTALIGN'd, making them space-efficient for on-disk storage
- Commonly used in tuple headers and page management operations