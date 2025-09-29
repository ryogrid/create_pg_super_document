# BlockIdGetBlockNumber

## Location
[src/include/storage/block.h:103-108](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/block.h#L103-L108)

## Overview
BlockIdGetBlockNumber is an inline function that retrieves the original 32-bit block number from a BlockIdData structure by reconstructing it from its high and low 16-bit components.

## Definition
```c
static inline BlockNumber BlockIdGetBlockNumber(const BlockIdData *blockId)
```

## Detailed Description
BlockIdGetBlockNumber performs the inverse operation of BlockIdSet by reconstructing a full 32-bit BlockNumber from the two 16-bit components stored in a BlockIdData structure. The function combines the bi_hi and bi_lo fields using bit shifting and bitwise OR operations to restore the original block number. This decoding is essential for converting the space-efficient on-disk BlockIdData format back to the computational BlockNumber format used in PostgreSQL's access methods and algorithms.

## Parameters / Member Variables
- `blockId`: Pointer to the BlockIdData structure from which to extract the block number (const-qualified)

## Dependencies
- Functions called/Symbols referenced:
  - [BlockIdData](BlockIdData.md) (structure type)
  - BlockNumber (return type)
- Called from (representative examples):
  - [ginRedoInsert](../g/ginRedoInsert.md)
  - [gin_desc](../g/gin_desc.md)
  - PostingItemGetBlockNumber
  - [ItemPointerGetBlockNumberNoCheck](../I/ItemPointerGetBlockNumberNoCheck.md)

## Notes and Other Information
- This is a static inline function defined in src/include/storage/block.h for optimal performance
- The reconstruction formula is: ((BlockNumber) bi_hi << 16) | ((BlockNumber) bi_lo)
- Returns a BlockNumber (32-bit unsigned integer) representing the original block number
- The function uses const-qualified parameter indicating it does not modify the input structure
- Commonly used in GIN index operations, WAL replay, and item pointer management
- Essential for converting between the space-efficient storage format and the computational format used in algorithms

## Simplified Source

```c
static inline BlockNumber BlockIdGetBlockNumber(const BlockIdData *blockId) {
    // Reconstruct 32-bit block number from high/low 16-bit components
    // Formula: (high_16_bits << 16) | low_16_bits
    return (((BlockNumber) blockId->bi_hi) << 16) | ((BlockNumber) blockId->bi_lo);
}
```