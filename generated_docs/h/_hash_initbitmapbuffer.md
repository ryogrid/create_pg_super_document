# _hash_initbitmapbuffer

## Location
[src/backend/access/hash/hashovfl.c:777-841](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashovfl.c#L777-L841)

## Overview
Initializes a new bitmap page in the PostgreSQL hash index, setting all bits to "1" to indicate that all corresponding overflow pages are "in use".

## Definition

```c
void
_hash_initbitmapbuffer(Buffer buf, uint16 bmsize, bool initpage)
```
## Detailed Description
This function initializes a bitmap page that tracks the allocation status of overflow pages in a hash index. The bitmap uses a "1" bit to indicate that an overflow page is allocated/in use, and "0" to indicate it's free. During initialization, all bits are set to "1" (0xFF pattern) since no overflow pages are initially free.

The function handles both the page initialization (if needed) and the setup of hash-specific metadata in the page's special space. It properly sets the page boundaries to make the page compressible for WAL logging.

## Parameters / Member Variables
- : Buffer containing the page to initialize as a bitmap page
- : Size in bytes of the bitmap data area 
- : Boolean flag indicating whether to perform basic page initialization

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md)
  - [_hash_pageinit](_hash_pageinit.md)
  - [BufferGetPageSize](../B/BufferGetPageSize.md)
  - HashPageGetOpaque
  - HashPageGetBitmap
  - memset
- Types/Constants referenced:
  - HashPageOpaque
  - InvalidBlockNumber
  - InvalidBucket
  - LH_BITMAP_PAGE
  - HASHO_PAGE_ID
  - PageHeader
- Called from (representative examples):
  - [hash_xlog_init_bitmap_page](hash_xlog_init_bitmap_page.md)
  - [hash_xlog_add_ovfl_page](hash_xlog_add_ovfl_page.md)
  - [_hash_addovflpage](_hash_addovflpage.md)
  - [_hash_init](_hash_init.md)

## Notes and Other Information
- All bitmap bits are initially set to "1" (indicating "in use") using memset with 0xFF
- The function sets pd_lower precisely to the end of bitmap data rather than equal to pd_upper to make the page appear compressible to the WAL system
- Bitmap pages have LH_BITMAP_PAGE flag in their special space opaque data
- The prevblkno and nextblkno fields are set to InvalidBlockNumber since bitmap pages don't participate in bucket chains

## Simplified Source

```c
void _hash_initbitmapbuffer(Buffer buf, uint16 bmsize, bool initpage) {
    Page pg = BufferGetPage(buf);

    // Initialize page if requested
    if (initpage) {
        _hash_pageinit(pg, BufferGetPageSize(buf));
    }

    // Setup page special space as bitmap page
    HashPageOpaque op = HashPageGetOpaque(pg);
    op->hasho_prevblkno = InvalidBlockNumber;
    op->hasho_nextblkno = InvalidBlockNumber;
    op->hasho_bucket = InvalidBucket;
    op->hasho_flag = LH_BITMAP_PAGE;
    op->hasho_page_id = HASHO_PAGE_ID;

    // Set all bitmap bits to 1 (indicating "in use")
    uint32 *freep = HashPageGetBitmap(pg);
    memset(freep, 0xFF, bmsize);

    // Set page boundary for compression
    ((PageHeader) pg)->pd_lower = ((char *) freep + bmsize) - (char *) pg;
}
```