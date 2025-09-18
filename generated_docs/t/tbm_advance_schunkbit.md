# tbm_advance_schunkbit

## Location
[src/backend/nodes/tidbitmap.c:941-970](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/tidbitmap.c#L941-L970)

## Overview
Advances the schunkbit pointer to the next set bit within a chunk entry during bitmap iteration.

## Definition
```c
static inline void tbm_advance_schunkbit(PagetableEntry *chunk, int *schunkbitp)
```

## Detailed Description
This static inline function is used during chunk iteration to efficiently locate the next page within a chunk that has tuple identifiers. It takes the current schunkbit position and advances it to the next bit that is set in the chunks bitmap words. The function uses bit manipulation techniques to examine individual bits within the appropriate bitmapword, utilizing WORDNUM and BITNUM macros to convert bit positions to word and bit indices. This advancement mechanism ensures that only pages with actual tuples are processed during iteration, skipping over empty pages within the chunk for optimal performance.

## Parameters / Member Variables
- `chunk`: Pointer to the PagetableEntry representing a chunk with multiple pages
- `schunkbitp`: Pointer to integer tracking the current bit position within the chunk

## Dependencies
- Functions called/Symbols referenced:
  - [PagetableEntry](../P/PagetableEntry.md) (structure type)
  - PAGES_PER_CHUNK (constant)
  - WORDNUM (macro)
  - BITNUM (macro)
  - bitmapword (type)
- Called from (representative examples):
  - [tbm_iterate](tbm_iterate.md) (src/backend/nodes/tidbitmap.c:987)
  - [tbm_shared_iterate](tbm_shared_iterate.md) (src/backend/nodes/tidbitmap.c:1079)

## Notes and Other Information
- This is a static inline function optimized for performance during bitmap iteration
- Modifies the schunkbitp parameter in-place to point to the next set bit
- Uses efficient bit manipulation with word and bit number macros
- Essential for chunk-based iteration where multiple pages are represented as bits in a single chunk entry
- Stops advancing when it finds a set bit or reaches PAGES_PER_CHUNK limit
- Part of the core bitmap iteration mechanism for handling chunked page representations