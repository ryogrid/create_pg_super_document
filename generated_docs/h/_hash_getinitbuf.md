# _hash_getinitbuf

## Location
[src/backend/access/hash/hashpage.c:135-156](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashpage.c#L135-L156)

## Overview
Gets a buffer for an existing block and initializes it from scratch, automatically applying page initialization for pages that need to be completely rewritten.

## Definition

```c
Buffer
_hash_getinitbuf(Relation rel, BlockNumber blkno)
```
## Detailed Description
This function is designed for accessing existing pages that are known to exist within the index's filesystem EOF but need to be completely reinitialized from scratch. Unlike _hash_getbuf which expects valid existing content, _hash_getinitbuf assumes the page content is invalid or unwanted and will be completely replaced.

The function performs these key operations:
1. Validates that the block number is not P_NEW (like other buffer functions)
2. Uses ReadBufferExtended with RBM_ZERO_AND_LOCK to zero the page content and acquire a write lock atomically
3. Automatically calls _hash_pageinit to set up the basic page structure
4. Returns a write-locked and pinned buffer ready for use

This is more efficient than reading an existing page and then overwriting it, since the RBM_ZERO_AND_LOCK flag avoids unnecessary I/O by zeroing the buffer without reading the old contents from disk.

## Parameters / Member Variables
- : The hash index relation containing the page
- : Block number of the existing page to initialize (must not be P_NEW)

## Dependencies
- Functions called/Symbols referenced:
  - [ReadBufferExtended](../R/ReadBufferExtended.md) (extended buffer read with special flags)
  - [_hash_pageinit](_hash_pageinit.md) (initializes basic page structure)
  - [BufferGetPage](../B/BufferGetPage.md), BufferGetPageSize (buffer access utilities)
  - MAIN_FORKNUM, RBM_ZERO_AND_LOCK, P_NEW (constants)
- Called from (representative examples):
  - [_hash_addovflpage](_hash_addovflpage.md) (when setting up new overflow pages)

## Notes and Other Information
- Uses RBM_ZERO_AND_LOCK for efficiency - avoids reading old page content from disk
- Automatically initializes page structure unlike the basic _hash_getbuf
- Returns buffer in write-locked state, suitable for immediate modification
- Can only be used for existing blocks within filesystem EOF, not for extending the index
- More efficient than read-modify-write cycle when entire page content will be replaced

## Simplified Source

```c
Buffer _hash_getinitbuf(Relation rel, BlockNumber blkno) {
    // P_NEW not allowed - this function only accesses existing pages
    if (blkno == P_NEW) {
        elog(ERROR, "hash AM does not use P_NEW");
    }

    // Read buffer with zero-and-lock mode (avoids reading old content)
    Buffer buf = ReadBufferExtended(rel, MAIN_FORKNUM, blkno,
                                   RBM_ZERO_AND_LOCK, NULL);

    // Initialize the page structure
    _hash_pageinit(BufferGetPage(buf), BufferGetPageSize(buf));

    return buf;  // Buffer is now write-locked, pinned, and initialized
}
```