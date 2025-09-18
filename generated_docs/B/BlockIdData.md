# BlockIdData

## Location
src/include/storage/block.h: 53 - 57

## Overview
BlockIdData is a storage-oriented data structure representing a block identifier used in PostgreSQL's on-disk structures, optimized for space efficiency through alignment considerations.

## Definition


## Detailed Description
BlockIdData serves as the on-disk storage representation of block numbers in PostgreSQL. Unlike BlockNumber (which is used for calculations in access method code), BlockIdData is specifically designed for storage in on-disk structures such as HeapTupleData and ItemPointerData. 

The structure splits a block number into two 16-bit components (bi_hi and bi_lo) to enable SHORTALIGN alignment, which is crucial for reducing space requirements in critical structures like the line pointer (ItemIdData) array on each page and tuple headers. This design decision prioritizes storage efficiency over computational convenience.

The bi_hi and bi_lo fields together represent a single block number, with bi_hi containing the high-order bits and bi_lo containing the low-order bits of the block identifier.

## Parameters / Member Variables
- : High-order 16 bits of the block number
- : Low-order 16 bits of the block number

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is a basic data structure)
- Called from (representative examples):
  - ItemPointerData (as ip_blkid member)
  - BlockId (pointer typedef)
  - BlockIdSet (function parameter)
  - BlockIdEquals (function parameter)
  - BlockIdGetBlockNumber (function parameter)
  - ginPlaceToPage
  - ginRedoInsert
  - hashtid
  - hashtidextended

## Notes and Other Information
- The separation between BlockIdData (storage) and BlockNumber (computation) exists primarily for alignment optimization
- SHORTALIGN capability allows structures containing BlockIdData to be more space-efficient
- This design is particularly important for ItemPointerData, which appears in every tuple header
- The structure is designed to avoid padding bytes that would waste storage space
- Used extensively in GIN index operations and tuple identification throughout the storage layer
- The typedef BlockId creates a pointer type (BlockIdData *) for passing block identifiers by reference