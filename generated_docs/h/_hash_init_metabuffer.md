# _hash_init_metabuffer

## Location
[src/backend/access/hash/hashpage.c:498-595](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashpage.c#L498-L595)

## Overview
Initializes the metadata page of a hash index with appropriate configuration based on estimated tuple count and fill factor.

## Definition


## Detailed Description
This function sets up the metadata page for a hash index, which is the control structure that manages the overall state of the hash index. It calculates the initial number of buckets based on the estimated tuple count and fill factor, initializes the page structure, and sets up the metadata fields including magic numbers, version information, bucket configuration, and bitmap management parameters. The function ensures proper page layout by setting pd_lower to prevent metadata loss during WAL compression.

## Parameters / Member Variables
- : Buffer containing the metadata page to be initialized
- : Estimated number of tuples that will be stored in the index
- : OID of the primary hash support function for forensic purposes
- : Fill factor determining how full buckets should be before splitting
- : Whether to initialize the page structure itself

## Dependencies
- Functions called/Symbols referenced:
  - _hash_get_totalbuckets
  - [_hash_spareindex](_hash_spareindex.md)
  - [_hash_pageinit](_hash_pageinit.md)
  - [BufferGetPage](../B/BufferGetPage.md)
  - [BufferGetPageSize](../B/BufferGetPageSize.md)
  - HashPageGetOpaque
  - HashPageGetMeta
  - HashGetMaxBitmapSize
  - [pg_leftmost_one_pos32](../p/pg_leftmost_one_pos32.md)
  - [pg_nextpower2_32](../p/pg_nextpower2_32.md)
  - MemSet
- Called from (representative examples):
  - [hash_xlog_init_meta_page](hash_xlog_init_meta_page.md)
  - [_hash_init](_hash_init.md)

## Notes and Other Information
- Always forces at least 2 bucket pages regardless of calculated requirements
- Upper limit of 0x40000000 buckets to prevent overflow issues
- Sets up initial spare page mapping for future table expansion
- Initializes bitmap size and shift parameters for efficient bitmap operations
- Critical for proper WAL recovery through pd_lower setting