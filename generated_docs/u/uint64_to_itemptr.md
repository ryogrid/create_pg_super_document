# uint64_to_itemptr

## Location
[src/backend/access/gin/ginpostinglist.c:102-114](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginpostinglist.c#L102-L114)

## Overview
Converts a 64-bit unsigned integer back to an ItemPointer, performing the inverse operation of itemptr_to_uint64.

## Definition
```c
static inline void uint64_to_itemptr(uint64 val, ItemPointer iptr)
```

## Detailed Description
This function reconstructs an ItemPointer from its 64-bit unsigned integer representation. It extracts the offset number from the lower bits and the block number from the upper bits, using MaxHeapTuplesPerPageBits to determine the bit boundaries. This is the inverse operation of itemptr_to_uint64 and is essential for decompressing GIN index posting lists back into their original ItemPointer format.

The function performs validation after reconstruction to ensure the resulting ItemPointer is valid.

## Parameters / Member Variables
- `val`: A 64-bit unsigned integer containing the packed representation of an ItemPointer
- `iptr`: An output ItemPointer that will be populated with the extracted block number and offset number

## Dependencies
- Functions called/Symbols referenced:
  - GinItemPointerSetOffsetNumber
  - GinItemPointerSetBlockNumber
  - MaxHeapTuplesPerPageBits
  - [ItemPointerIsValid](../I/ItemPointerIsValid.md)
- Called from (representative examples):
  - [ginPostingListDecodeAllSegments](../g/ginPostingListDecodeAllSegments.md)

## Notes and Other Information
- This is a static inline function for optimal performance during GIN index decompression operations
- The function uses bit masking and shifting to extract the offset number from the lower MaxHeapTuplesPerPageBits bits
- The remaining upper bits contain the block number
- Used primarily during GIN index posting list decompression to restore the original ItemPointer format
- The function modifies the provided ItemPointer in-place rather than returning a new one