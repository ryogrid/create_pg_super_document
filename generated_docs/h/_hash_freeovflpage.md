# _hash_freeovflpage

## Location
[src/backend/access/hash/hashovfl.c:490-776](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashovfl.c#L490-L776)

## Overview
Removes an overflow page from its bucket chain and marks it as free in the bitmap, while transferring any remaining tuples to a designated write page.

## Definition
```c
BlockNumber _hash_freeovflpage(Relation rel, Buffer bucketbuf, Buffer ovflbuf,
                              Buffer wbuf, IndexTuple *itups, OffsetNumber *itup_offsets,
                              Size *tups_size, uint16 nitups,
                              BufferAccessStrategy bstrategy)
```

## Detailed Description
This function performs the complete removal of an overflow page from a hash index bucket chain, which involves several critical operations:

1. **Chain maintenance**: Updates the doubly-linked list of overflow pages by fixing the previous and next page pointers
2. **Tuple migration**: Moves any remaining tuples from the overflow page to the designated write buffer
3. **Page cleanup**: Reinitializes the overflow page to an unused state with proper special space
4. **Bitmap management**: Clears the corresponding bit in the bitmap page and updates the firstfree pointer if necessary
5. **WAL logging**: Creates comprehensive WAL records to ensure atomicity during recovery

The function is designed to be called during VACUUM operations and bucket squeeze operations, using lock chaining to avoid deadlocks with concurrent operations.

## Parameters / Member Variables
- `rel`: The hash index relation being modified
- `bucketbuf`: Buffer for the primary bucket page
- `ovflbuf`: Buffer for the overflow page being freed (must be write-locked on entry)
- `wbuf`: Write buffer where tuples from the overflow page will be moved
- `itups`: Array of index tuples to be moved to the write buffer
- `itup_offsets`: Array of offset numbers for the tuples
- `tups_size`: Array of sizes for each tuple
- `nitups`: Number of tuples to move
- `bstrategy`: Buffer access strategy for controlling page fetches

## Dependencies
- Functions called/Symbols referenced:
  - [_hash_checkpage](_hash_checkpage.md) (page validation)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)/BufferGetPage (buffer access)
  - HashPageGetOpaque/HashPageGetMeta/HashPageGetBitmap (page structure access)
  - [_hash_getbuf_with_strategy](_hash_getbuf_with_strategy.md)/_hash_getbuf (buffer management with strategy)
  - [_hash_ovflblkno_to_bitno](_hash_ovflblkno_to_bitno.md) (block number to bit number conversion)
  - [_hash_pgaddmultitup](_hash_pgaddmultitup.md) (adding multiple tuples to a page)
  - [_hash_pageinit](_hash_pageinit.md) (page initialization)
  - CLRBIT/ISSET (bitmap manipulation)
  - XLog functions (WAL logging)
  - [_hash_relbuf](_hash_relbuf.md) (buffer release)
- Called from (representative examples):
  - [_hash_squeezebucket](_hash_squeezebucket.md) (during bucket squeeze operations)
  - HASHNProcs (hash index procedure definitions)

## Notes and Other Information
- The function releases the write lock on ovflbuf before exiting
- Uses lock chaining strategy to prevent deadlocks during concurrent operations
- Returns the block number of the page that followed the freed page in the bucket chain
- Includes comprehensive WAL logging with support for multiple buffer registrations
- The bstrategy parameter controls buffer access for bucket pages but is intentionally not used for metapage and bitmap access
- Performs validation to ensure the overflow bit number is valid before clearing it
- Updates the hashm_firstfree pointer in metadata when the freed page becomes the earliest free page
- Critical section ensures atomicity of all modifications across multiple pages