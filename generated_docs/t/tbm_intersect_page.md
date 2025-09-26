# tbm_intersect_page

## Location
src/backend/nodes/tidbitmap.c: 589 - 669

## Overview
Intersects a single page entry from one TIDBitmap with another during intersection operations, returning whether the page becomes empty and should be deleted.

## Definition
```c
static bool tbm_intersect_page(TIDBitmap *a, PagetableEntry *apage, const TIDBitmap *b)
```

## Detailed Description
The `tbm_intersect_page` function handles the intersection of individual page entries during TIDBitmap intersection operations. It supports three different scenarios based on the nature of the source and target page entries:

1. **Chunk intersection**: For lossy chunks (ischunk=true), it examines each bit in the chunk's bitmap words, clearing bits for pages that don't exist in the second bitmap. Returns true if all bits are cleared.

2. **Lossy target in b**: If the target page exists as lossy in bitmap b, all tuples in the source page are potentially valid matches, but the recheck flag is set to indicate that verification is needed during scanning.

3. **Exact page intersection**: For exact pages present in both bitmaps, it performs bitwise AND operations on the bitmap words to keep only common tuple locations. The recheck flags are combined using OR logic.

The function returns true if the resulting page becomes empty and should be deleted from the bitmap.

## Parameters / Member Variables
- `a`: Source TIDBitmap being modified during intersection
- `apage`: PagetableEntry from bitmap a to be intersected
- `b`: Target TIDBitmap used for comparison (remains unchanged)

## Dependencies
- Functions called/Symbols referenced:
  - tbm_page_is_lossy
  - tbm_find_pageentry
  - WORDS_PER_CHUNK (constant)
  - WORDS_PER_PAGE (constant)
  - BITS_PER_BITMAPWORD (constant)
  - bitmapword (type)
  - PagetableEntry (type)
- Called from (representative examples):
  - tbm_intersect (in src/backend/nodes/tidbitmap.c:549, 568)
  - TBMSharedIterator (in src/backend/nodes/tidbitmap.c:230)

## Notes and Other Information
- This is a static function, only accessible within the tidbitmap.c module
- Returns true when the page becomes empty and should be removed from the bitmap
- Handles the complexity of intersecting different page representation types (exact vs lossy)
- Chunk processing uses bit manipulation to efficiently clear non-matching pages
- The recheck flag propagation ensures proper handling of uncertain matches
- Lossy pages in the target bitmap preserve all source tuples but require rechecking
- For exact intersections, only tuple locations present in both pages are retained