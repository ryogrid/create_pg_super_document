# _hash_getnewbuf

## Location
[src/backend/access/hash/hashpage.c:198-238](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashpage.c#L198-L238)

## Overview
Gets a new buffer at the end of the index by extending the relation, handling both filesystem extension and recovery from incomplete prior extensions.

## Definition

```c
Buffer
_hash_getnewbuf(Relation rel, BlockNumber blkno, ForkNumber forkNum)
```
## Detailed Description
This function is responsible for extending a hash index by adding new pages at the end of the relation. Unlike other buffer functions that work with existing pages, _hash_getnewbuf specifically handles index growth and must deal with the complexities of filesystem extension.

The function handles two scenarios:
1. **Normal extension**: When blkno equals the current number of blocks, it uses ExtendBufferedRel to physically extend the relation and get a new buffer
2. **Recovery case**: When blkno is less than nblocks (due to a previous crash after filesystem extension but before metadata update), it uses ReadBufferExtended with RBM_ZERO_AND_LOCK

Key safety mechanisms:
- Validates that blkno is not P_NEW (consistency with other hash buffer functions)  
- Ensures pages are allocated contiguously (blkno cannot exceed nblocks + 1)
- Verifies that ExtendBufferedRel returns the expected block number
- Always initializes the new page with _hash_pageinit

The caller must hold write lock on the metapage to ensure only one process extends the index at a time.

## Parameters / Member Variables
- : The hash index relation to extend
- : Block number of the new page to allocate (must be ≤ current nblocks)
- : Fork number (typically MAIN_FORKNUM for the main relation data)

## Dependencies
- Functions called/Symbols referenced:
  - [RelationGetNumberOfBlocksInFork](../R/RelationGetNumberOfBlocksInFork.md) (gets current relation size)
  - [ExtendBufferedRel](../E/ExtendBufferedRel.md) (extends relation and gets buffer)
  - [ReadBufferExtended](../R/ReadBufferExtended.md) (reads existing block with special flags)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md) (gets block number from buffer)
  - [_hash_pageinit](_hash_pageinit.md) (initializes page structure)
  - [BufferGetPage](../B/BufferGetPage.md), BufferGetPageSize (buffer utilities)
  - BMR_REL, EB_LOCK_FIRST, EB_SKIP_EXTENSION_LOCK, RBM_ZERO_AND_LOCK (constants)
- Called from (representative examples):
  - [_hash_addovflpage](_hash_addovflpage.md) (when adding overflow pages)
  - [_hash_init](_hash_init.md) (during index creation)
  - [_hash_expandtable](_hash_expandtable.md) (during hash table expansion)

## Notes and Other Information
- Only function in this group that can extend the index filesystem size
- Handles crash recovery scenarios gracefully
- Requires caller coordination (typically metapage write lock) to prevent concurrent extensions
- Returns write-locked and initialized buffer ready for use
- Critical for hash index growth operations like bucket splitting and overflow page allocation