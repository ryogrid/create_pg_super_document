# tbm_union_page

## Location
[src/backend/nodes/tidbitmap.c:481-539](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/tidbitmap.c#L481-L539)

## Overview
Merges a single page entry from one TIDBitmap into another during union operations, handling both exact and lossy page representations.

## Definition
```c
static void tbm_union_page(TIDBitmap *a, const PagetableEntry *bpage)
```

## Detailed Description
The `tbm_union_page` function is a core component of the TIDBitmap union operation that handles the merging of individual page entries. It supports three different scenarios based on the nature of the source page entry:

1. **Chunk processing**: If the source page is a lossy chunk (ischunk=true), it iterates through each bit in the chunk's bitmap words, marking corresponding pages as lossy in the target bitmap.

2. **Lossy target check**: If the target page is already lossy, no action is needed since lossy representation already covers all possible tuples on that page.

3. **Exact page merging**: For exact pages, it performs bitwise OR operations on the bitmap words to combine the exact tuple locations, and merges recheck flags.

The function ensures memory limits are respected by calling `tbm_lossify` if the target bitmap exceeds its maximum entries.

## Parameters / Member Variables
- `a`: Target TIDBitmap that will be modified to include the page
- `bpage`: Source PagetableEntry to be merged into the target bitmap

## Dependencies
- Functions called/Symbols referenced:
  - tbm_mark_page_lossy
  - tbm_page_is_lossy  
  - tbm_get_pageentry
  - tbm_lossify
  - WORDS_PER_CHUNK (constant)
  - WORDS_PER_PAGE (constant)
  - BITS_PER_BITMAPWORD (constant)
  - bitmapword (type)
  - PagetableEntry (type)
- Called from (representative examples):
  - tbm_union (in src/backend/nodes/tidbitmap.c:466, 475)
  - TBMSharedIterator (in src/backend/nodes/tidbitmap.c:229)

## Notes and Other Information
- This is a static function, only accessible within the tidbitmap.c module
- Handles the complexity of merging different page representation types (exact vs lossy)
- Chunk processing uses bit manipulation to efficiently identify which pages should be marked lossy
- The recheck flag is preserved and merged when combining exact pages
- Memory management is automatically handled to prevent unbounded growth