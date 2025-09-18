# _hash_alloc_buckets

## Location
src/backend/access/hash/hashpage.c: 992 - 1072

## Overview
Allocates a new splitpoint's worth of bucket pages by extending the logical EOF and writing a properly initialized page at the end of the range.

## Definition
```c
static bool _hash_alloc_buckets(Relation rel, BlockNumber firstblock, uint32 nblocks)
```

## Detailed Description
This function extends the hash index file to accommodate a new splitpoint's worth of bucket pages. Rather than initializing each individual page, it uses a filesystem optimization technique by writing only the last page of the range as a properly initialized zero page, allowing the filesystem to handle the intervening pages as a "hole" that reads as zeroes. This approach reduces immediate disk allocation while maintaining file integrity. The function includes overflow protection for BlockNumber calculations and ensures proper WAL logging when required. The allocated pages are marked as unused until they are individually initialized by _hash_expandtable().

## Parameters / Member Variables
- `rel`: The hash index relation being extended
- `firstblock`: Starting block number for the new bucket pages
- `nblocks`: Number of blocks to allocate for the splitpoint

## Dependencies
- Functions called/Symbols referenced:
  - [_hash_pageinit](_hash_pageinit.md)
  - HashPageGetOpaque
  - RelationNeedsWAL
  - [log_newpage](../l/log_newpage.md)
  - [PageSetChecksumInplace](../P/PageSetChecksumInplace.md)
  - [smgrextend](../s/smgrextend.md)
  - RelationGetSmgr
  - MAIN_FORKNUM
  - InvalidBlockNumber
  - InvalidBucket
  - LH_UNUSED_PAGE
  - HASHO_PAGE_ID
- Called from (representative examples):
  - [_hash_expandtable](_hash_expandtable.md)

## Notes and Other Information
- Returns false if BlockNumber overflow occurs, preventing index corruption
- Uses filesystem "hole" optimization to reduce immediate disk allocation
- Only writes the last page of the range, allowing intervening pages to be zero-filled by the filesystem
- Properly initializes special space for tool compatibility (e.g., pageinspect)
- Executed while holding metapage lock, which may cause some performance concerns
- Could potentially use LockRelationForExtension for better concurrency
- Infrequent operation, so performance impact is generally acceptable