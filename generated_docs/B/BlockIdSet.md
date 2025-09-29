# BlockIdSet

## Location
[src/include/storage/block.h:81-91](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/block.h#L81-L91)

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
  - [BlockIdData](BlockIdData.md) (structure type)
- Called from (representative examples):
  - [ginPlaceToPage](../g/ginPlaceToPage.md)
  - PostingItemSetBlockNumber
  - [ItemPointerSet](../I/ItemPointerSet.md)
  - [ItemPointerSetBlockNumber](../I/ItemPointerSetBlockNumber.md)
  - [ItemPointerSetInvalid](../I/ItemPointerSetInvalid.md)

## Notes and Other Information
- This is a static inline function defined in src/include/storage/block.h for optimal performance
- The encoding splits a 32-bit BlockNumber into: bi_hi = blockNumber >> 16, bi_lo = blockNumber & 0xffff
- Part of PostgreSQL's block management system for efficient storage space utilization
- [BlockIdData](BlockIdData.md) structures can be SHORTALIGN'd, making them space-efficient for on-disk storage
- Commonly used in tuple headers and page management operations

## Simplified Source

```c
static inline void
BlockIdSet(BlockIdData *blockId, BlockNumber blockNumber)
{
    // Split 32-bit block number into high and low 16-bit components
    blockId->bi_hi = blockNumber >> 16;  // Upper 16 bits
    blockId->bi_lo = blockNumber & 0xffff;  // Lower 16 bits
}
```