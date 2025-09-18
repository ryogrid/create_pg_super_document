# _hash_initbuf

## Location
[src/backend/access/hash/hashpage.c:157-197](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashpage.c#L157-L197)

## Overview
Initializes a hash page buffer with bucket-specific metadata, setting up the page opaque area with bucket number, flags, and other hash-specific information.

## Definition


## Detailed Description
This function initializes a buffer that has already been allocated for use as a hash index page. Unlike the buffer allocation functions, _hash_initbuf operates on an existing buffer and focuses specifically on setting up the hash-specific metadata in the page's opaque area.

The function performs these operations:
1. Optionally calls _hash_pageinit if initpage is true to set up basic page structure
2. Accesses the page's opaque area (hash-specific metadata region)
3. Sets up hash-specific fields including bucket number, flags, and validation information
4. Stores the current max_bucket value in hasho_prevblkno for cache validation purposes
5. Initializes next block pointer and page ID

The hasho_prevblkno field is particularly important as it stores the max_bucket value at the time of page creation, which is later used by _hash_getbucketbuf_from_hashkey() to validate cached metadata.

## Parameters / Member Variables
- : Buffer to initialize (must already be allocated and locked)
- : Current maximum bucket number in the hash table
- : The bucket number this page belongs to
- : Page type flags (e.g., primary bucket page, overflow page)
- : Whether to perform basic page initialization first

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md) (gets page from buffer)
  - [_hash_pageinit](_hash_pageinit.md) (basic page initialization, if initpage is true)
  - [BufferGetPageSize](../B/BufferGetPageSize.md) (gets buffer page size)
  - HashPageGetOpaque (accesses hash-specific page metadata)
  - HASHO_PAGE_ID, InvalidBlockNumber (constants)
- Called from (representative examples):
  - [hash_xlog_add_ovfl_page](hash_xlog_add_ovfl_page.md) (during WAL recovery of overflow page addition)
  - [hash_xlog_split_allocate_page](hash_xlog_split_allocate_page.md) (during WAL recovery of page splitting)
  - [_hash_init](_hash_init.md) (during hash index creation)

## Notes and Other Information
- Works with pre-allocated buffers, unlike the buffer allocation functions
- The hasho_prevblkno field serves a validation purpose rather than actual linking
- Can optionally skip page initialization if the page structure is already set up
- Used primarily during index creation and WAL recovery operations
- Sets up hash-specific metadata that distinguishes hash pages from other page types