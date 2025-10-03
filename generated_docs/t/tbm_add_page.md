# tbm_add_page

## Location
[src/backend/nodes/tidbitmap.c:443-457](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/tidbitmap.c#L443-L457)

## Overview
Adds a whole page to a TIDBitmap, marking it as requiring a recheck when scanned.

## Definition

```c
void
tbm_add_page(TIDBitmap *tbm, BlockNumber pageno)
```
## Detailed Description
The  function is used to add an entire page to a TIDBitmap structure. When a page is added through this function, it means that all tuples on that page are considered to potentially match the query condition, but they will need to be rechecked during the actual scan. This is a "lossy" representation where individual tuple locations are not tracked, only the page number.

The function works by first marking the page as lossy using , then checking if the bitmap has exceeded its memory limit. If the limit is exceeded, it calls  to convert some exact page entries to lossy format to stay within memory bounds.

## Parameters / Member Variables
- `*tbm`: Pointer to the TIDBitmap structure to which the page will be added
- `pageno`: Block number of the page to be added to the bitmap
## Dependencies
- Functions called/Symbols referenced:
  - [tbm_mark_page_lossy](tbm_mark_page_lossy.md)
  - [tbm_lossify](tbm_lossify.md)
  - [TIDBitmap](../T/TIDBitmap.md) (type)
- Called from (representative examples):
  - [bringetbitmap](../b/bringetbitmap.md) (in src/backend/access/brin/brin.c:923)
  - [gingetbitmap](../g/gingetbitmap.md) (in src/backend/access/gin/ginget.c:1962)

## Notes and Other Information
- This function is typically used by index access methods when they cannot determine exact tuple locations but know that an entire page contains matching tuples
- The lossy representation trades precision for memory efficiency
- Pages added through this function will have the recheck flag set when scanned
- Memory management is automatically handled through the tbm_lossify mechanism when limits are exceeded