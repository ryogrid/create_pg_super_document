# tbm_extract_page_tuple

## Location
src/backend/nodes/tidbitmap.c: 911 - 940

## Overview
Extracts tuple offset numbers from a bitmap page entry and stores them in a TBMIterateResult structure.

## Definition
```c
static inline int tbm_extract_page_tuple(PagetableEntry *page, TBMIterateResult *output)
```

## Detailed Description
This static inline function processes a single page entry from a TIDBitmap to extract the individual tuple offset numbers that are set in the pages bitmap. It iterates through each bitmapword in the page entry, examining each bit to identify which tuple offsets are present. The function uses bit manipulation techniques to efficiently scan through the bitmap words, extracting offset numbers and storing them in the output structures offsets array. The extraction process converts the compact bitmap representation back into explicit tuple offset numbers that can be used for tuple retrieval.

## Parameters / Member Variables
- `page`: Pointer to the PagetableEntry containing the bitmap data for a specific database page
- `output`: Pointer to TBMIterateResult structure where extracted tuple offsets will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [PagetableEntry](../P/PagetableEntry.md) (structure type)
  - TBMIterateResult (structure type)
  - WORDS_PER_PAGE (constant)
  - bitmapword (type)
  - BITS_PER_BITMAPWORD (constant)
- Called from (representative examples):
  - [tbm_iterate](tbm_iterate.md) (src/backend/nodes/tidbitmap.c:1032)
  - [tbm_shared_iterate](tbm_shared_iterate.md) (src/backend/nodes/tidbitmap.c:1121)

## Notes and Other Information
- This is a static inline function for performance optimization during bitmap iteration
- Returns the number of tuples extracted from the page bitmap
- Uses efficient bit manipulation with right shifts and bitwise AND operations
- Converts bit positions back to tuple offset numbers by adding the base offset for each word
- The function processes the bitmap word by word, then bit by bit within each word
- Essential component of both private and shared bitmap iteration mechanisms