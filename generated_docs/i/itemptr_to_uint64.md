# itemptr_to_uint64

## Location
src/backend/access/gin/ginpostinglist.c: 87 - 101

## Overview
Converts an ItemPointer to a 64-bit unsigned integer representation for efficient storage and comparison in GIN index posting lists.

## Definition


## Detailed Description
This function converts a PostgreSQL ItemPointer (which contains a block number and offset number) into a compact 64-bit unsigned integer representation. The conversion packs the block number in the upper bits and the offset number in the lower bits, using MaxHeapTuplesPerPageBits to determine the bit allocation. This compact representation is essential for efficient storage and processing of posting lists in GIN indexes, where many ItemPointers need to be stored and compared.

The function performs validation to ensure the ItemPointer is valid and that the offset number fits within the allocated bit space.

## Parameters / Member Variables
- `iptr`: A const ItemPointer that points to a specific tuple location (block number + offset number within that block)

## Dependencies
- Functions called/Symbols referenced:
  - ItemPointerIsValid
  - GinItemPointerGetBlockNumber
  - GinItemPointerGetOffsetNumber
  - MaxHeapTuplesPerPageBits
- Called from (representative examples):
  - ginCompressPostingList
  - ginPostingListDecodeAllSegments

## Notes and Other Information
- This is a static inline function for optimal performance since it's called frequently during GIN index operations
- The function assumes that the offset number fits within MaxHeapTuplesPerPageBits bits
- The bit layout places block numbers in higher-order bits and offset numbers in lower-order bits
- Used primarily in GIN index posting list compression and decompression operations