# xl_hash_init_bitmap_page

## Location
[src/include/access/hash_xlog.h:234-237](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/hash_xlog.h#L234-L237)

## Overview
The xl_hash_init_bitmap_page struct represents the WAL record data for hash index bitmap page initialization operations, used to log the creation and setup of bitmap pages that track overflow page allocation.

## Definition

```c
typedef struct xl_hash_init_bitmap_page
{
	uint16		bmsize;
} xl_hash_init_bitmap_page;
```
## Detailed Description
This structure contains the necessary information to perform or replay hash index bitmap page initialization operations during WAL recovery. Bitmap pages in hash indexes are used to track the allocation status of overflow pages, helping to efficiently manage space allocation and deallocation. The bitmap page initialization occurs during hash index creation or when new bitmap pages need to be added to accommodate more overflow pages.

The operation involves 2 backup blocks:
- Backup Blk 0: bitmap page
- Backup Blk 1: meta page

## Parameters / Member Variables
- : The size of the bitmap in the bitmap page, indicating how many overflow pages this bitmap page can track

## Dependencies
- Functions called/Symbols referenced:
  - uint16 (type)
- Called from (representative examples):
  - [hash_xlog_init_bitmap_page](../h/hash_xlog_init_bitmap_page.md) (WAL replay function for bitmap page initialization)
  - [_hash_init](../h/_hash_init.md) (hash index initialization function)
  - [hash_desc](../h/hash_desc.md) (hash WAL record description function)
  - SizeOfHashInitBitmapPage (macro for calculating structure size)

## Notes and Other Information
- This is specifically used for XLOG_HASH_INIT_BITMAP_PAGE WAL record type
- Bitmap pages are essential for hash index overflow page management, tracking which overflow pages are allocated and which are free
- The bmsize field determines the capacity of the bitmap page to track overflow pages
- This operation typically occurs during initial hash index creation or when expanding the index's overflow page tracking capacity
- Part of PostgreSQL's hash index access method implementation for managing overflow page allocation
- Critical for ensuring proper recovery of hash index bitmap page creation after a crash
- The meta page is also involved in the operation as it may need updates to reference the new bitmap page
- Defined in src/include/access/hash_xlog.h at lines 234-237