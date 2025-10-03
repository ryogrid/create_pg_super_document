# _hash_pageinit

## Location
[src/backend/access/hash/hashpage.c:596-613](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashpage.c#L596-L613)

## Overview
Initializes a new hash index page by setting up the basic page structure with hash-specific opaque data space.

## Definition
```c
void _hash_pageinit(Page page, Size size)
```

## Detailed Description
This function is a simple wrapper around the standard PageInit function that initializes a page with hash index-specific parameters. It sets up the page header and reserves space for HashPageOpaqueData at the end of the page, which contains hash-specific metadata like bucket information, page type flags, and link pointers for bucket chains.

## Parameters / Member Variables
- `page`: Pointer to the page memory to be initialized
- `size`: Size of the page in bytes

## Dependencies
- Functions called/Symbols referenced:
  - [PageInit](../P/PageInit.md)
  - [HashPageOpaqueData](../H/HashPageOpaqueData.md)
- Called from (representative examples):
  - [_hash_getinitbuf](_hash_getinitbuf.md)
  - [_hash_initbuf](_hash_initbuf.md)
  - [_hash_getnewbuf](_hash_getnewbuf.md)
  - [_hash_init_metabuffer](_hash_init_metabuffer.md)
  - [_hash_alloc_buckets](_hash_alloc_buckets.md)
  - [_hash_initbitmapbuffer](_hash_initbitmapbuffer.md)
  - [_hash_freeovflpage](_hash_freeovflpage.md)
  - [hash_xlog_squeeze_page](hash_xlog_squeeze_page.md)

## Notes and Other Information
- Essential initialization step for all hash index pages
- Reserves space for hash-specific opaque data structure
- Used consistently across all hash page creation scenarios
- Simple but critical function ensuring proper page layout for hash operations

## Simplified Source

```c
void _hash_pageinit(Page page, Size size)
{
    // Initialize page with hash-specific opaque data space
    PageInit(page, size, sizeof(HashPageOpaqueData));
}
```