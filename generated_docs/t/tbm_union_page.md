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
  - [tbm_mark_page_lossy](tbm_mark_page_lossy.md)
  - [tbm_page_is_lossy](tbm_page_is_lossy.md)  
  - [tbm_get_pageentry](tbm_get_pageentry.md)
  - [tbm_lossify](tbm_lossify.md)
  - WORDS_PER_CHUNK (constant)
  - WORDS_PER_PAGE (constant)
  - BITS_PER_BITMAPWORD (constant)
  - bitmapword (type)
  - [PagetableEntry](../P/PagetableEntry.md) (type)
- Called from (representative examples):
  - [tbm_union](tbm_union.md) (in src/backend/nodes/tidbitmap.c:466, 475)
  - [TBMSharedIterator](../T/TBMSharedIterator.md) (in src/backend/nodes/tidbitmap.c:229)

## Notes and Other Information
- This is a static function, only accessible within the tidbitmap.c module
- Handles the complexity of merging different page representation types (exact vs lossy)
- Chunk processing uses bit manipulation to efficiently identify which pages should be marked lossy
- The recheck flag is preserved and merged when combining exact pages
- Memory management is automatically handled to prevent unbounded growth

## Simplified Source

```c
static void
tbm_union_page(TIDBitmap *a, const PagetableEntry *bpage)
{
    if (bpage->ischunk)
    {
        // Process chunk: mark each indicated page as lossy
        for (int wordnum = 0; wordnum < WORDS_PER_CHUNK; wordnum++)
        {
            bitmapword w = bpage->words[wordnum];
            if (w != 0)
            {
                BlockNumber pg = bpage->blockno + (wordnum * BITS_PER_BITMAPWORD);

                // Mark each set bit as lossy page
                while (w != 0)
                {
                    if (w & 1)
                        tbm_mark_page_lossy(a, pg);
                    pg++;
                    w >>= 1;
                }
            }
        }
    }
    else if (tbm_page_is_lossy(a, bpage->blockno))
    {
        // Target page already lossy - nothing to do
        return;
    }
    else
    {
        // Exact page union
        PagetableEntry *apage = tbm_get_pageentry(a, bpage->blockno);

        if (apage->ischunk)
        {
            // Target became lossy chunk - set bit for this page
            apage->words[0] |= ((bitmapword) 1 << 0);
        }
        else
        {
            // Bitwise OR operation for exact union
            for (int wordnum = 0; wordnum < WORDS_PER_PAGE; wordnum++)
                apage->words[wordnum] |= bpage->words[wordnum];
            apage->recheck |= bpage->recheck;
        }
    }

    // Check if we need to lossify due to memory limits
    if (a->nentries > a->maxentries)
        tbm_lossify(a);
}
```

This simplified version shows the three union scenarios: chunk processing with lossy page marking, exact page bitwise OR operations, and automatic memory management through lossification.