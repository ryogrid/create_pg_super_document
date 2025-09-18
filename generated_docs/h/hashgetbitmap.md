# hashgetbitmap

## Location
src/backend/access/hash/hash.c: 335 - 366

## Overview
Retrieves all tuples from a hash index scan at once and stores them in a TID bitmap for efficient bulk processing.

## Definition
```c
int64 hashgetbitmap(IndexScanDesc scan, TIDBitmap *tbm)
```

## Detailed Description
The hashgetbitmap function implements a bitmap index scan for hash indexes, which is an optimization for retrieving multiple tuples efficiently. Unlike hashgettuple which returns one tuple at a time, this function scans through the entire hash index in one pass and collects all matching tuple IDs (TIDs) into a bitmap structure.

The function performs a complete forward scan of the hash index, starting with _hash_first and continuing with _hash_next until all qualifying tuples are processed. Each found tuple's heap TID is added to the provided TIDBitmap. Since the underlying _hash_first and _hash_next functions already handle dead tuple elimination when scan->ignore_killed_tuples is true, no additional filtering is needed.

## Parameters / Member Variables
- `scan`: IndexScanDesc structure containing the scan state and configuration
- `tbm`: TIDBitmap structure where matching tuple IDs will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [_hash_first](_hash_first.md)
  - [_hash_next](_hash_next.md)
  - [tbm_add_tuples](../t/tbm_add_tuples.md)
  - ForwardScanDirection
  - HashScanOpaque
  - [HashScanPosItem](../H/HashScanPosItem.md)
- Called from (representative examples):
  - [hashhandler](hashhandler.md) (hash access method handler)
  - Referenced in HASHNProcs (hash index procedure array)

## Notes and Other Information
- Always scans in forward direction regardless of scan direction preference
- Returns the total number of tuples found and added to the bitmap
- More efficient than repeated hashgettuple calls for bulk operations
- Dead tuple elimination is handled automatically by the underlying scan functions
- Used primarily for bitmap heap scans where multiple index conditions are combined