# ginCompressPostingList

## Location
[src/backend/access/gin/ginpostinglist.c:197-283](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginpostinglist.c#L197-L283)

## Overview
Compresses an array of ItemPointers into a space-efficient GIN posting list format using delta encoding and variable-length byte encoding.

## Definition
```c
GinPostingList * ginCompressPostingList(const ItemPointer ipd, int nipd, int maxsize, int *nwritten)
```

## Detailed Description
This function implements the core compression algorithm for GIN index posting lists. It takes an array of sorted ItemPointers and produces a compressed representation that significantly reduces storage space. The compression uses several techniques:

1. **First Item Storage**: The first ItemPointer is stored uncompressed as a reference point
2. **Delta Encoding**: Subsequent items are stored as deltas (differences) from the previous item
3. **Variable-Length Encoding**: Deltas are encoded using varbyte encoding to use fewer bytes for smaller differences
4. **Size Limiting**: The function respects a maximum output size, potentially encoding only a subset of input items

The function ensures the output is short-aligned and includes padding bytes as needed. In debug builds, it can verify the encoding by decoding and comparing with the original data.

## Parameters / Member Variables
- `ipd`: Array of ItemPointers to compress (must be sorted in ascending order)
- `nipd`: Number of ItemPointers in the input array
- `maxsize`: Maximum size in bytes for the compressed output
- `nwritten`: Output parameter returning the number of items that were successfully encoded

## Dependencies
- Functions called/Symbols referenced:
  - [itemptr_to_uint64](../i/itemptr_to_uint64.md)
  - [encode_varbyte](../e/encode_varbyte.md)
  - [palloc](../p/palloc.md)
  - SHORTALIGN_DOWN
  - SHORTALIGN
  - MaxBytesPerInteger
  - SizeOfGinPostingList
  - [ginPostingListDecode](ginPostingListDecode.md) (in debug builds)
- Called from (representative examples):
  - [ginVacuumPostingTreeLeaf](ginVacuumPostingTreeLeaf.md)
  - [leafRepackItems](../l/leafRepackItems.md)
  - [addItemPointersToLeafTuple](../a/addItemPointersToLeafTuple.md)
  - [buildFreshLeafTuple](../b/buildFreshLeafTuple.md)

## Notes and Other Information
- Returns a palloc'd GinPostingList that must be freed by the caller
- The input ItemPointer array must be sorted in ascending order for delta encoding to work correctly
- Uses MaxBytesPerInteger (typically 7) as a safety buffer when checking remaining space
- The compression ratio depends on the clustering of the ItemPointers - closer items compress better
- In debug builds with CHECK_ENCODING_ROUNDTRIP defined, verifies encoding correctness by decoding
- The function handles partial encoding when the output size limit is reached
- Output structure is short-aligned to meet PostgreSQL alignment requirements
- Used extensively throughout GIN index operations for space-efficient storage of tuple location lists