# RevmapContents

## Location
src/include/access/brin_page.h: 78 - 86

## Overview
RevmapContents is a structure that defines the contents of BRIN reverse map pages, containing an array of item pointers that map heap block ranges to their corresponding BRIN index tuples.

## Definition
```c
typedef struct RevmapContents
{
    /*
     * This array will fill all available space on the page.  It should be
     * declared [FLEXIBLE_ARRAY_MEMBER], but for some reason you can't do that
     * in an otherwise-empty struct.
     */
    ItemPointerData rm_tids[1];
} RevmapContents;
```

## Detailed Description
RevmapContents represents the data structure used in BRIN reverse map pages to store the mapping between heap block ranges and their corresponding BRIN index tuples. The reverse map is a critical component of BRIN indexes that allows efficient lookup of which BRIN tuple summarizes a given heap block range.

The structure uses a flexible array approach where the rm_tids array is designed to fill all available space on the page. Although ideally it would use a flexible array member, the implementation uses a fixed-size array of 1 element due to C language constraints with otherwise-empty structs. In practice, this array expands to fill the entire available page space.

Each ItemPointerData in the array points to a BRIN tuple that summarizes a specific range of heap blocks, providing the reverse mapping functionality that makes BRIN indexes efficient for range queries.

## Parameters / Member Variables
- `rm_tids`: Array of ItemPointerData structures, each containing a tuple identifier that points to a BRIN index tuple responsible for summarizing a specific range of heap blocks

## Dependencies
- Functions called/Symbols referenced:
  - ItemPointerData (PostgreSQL item pointer type)
- Called from (representative examples):
  - brinSetHeapBlockItemptr (in brin_revmap.c:158, 164)
  - brinGetTupleForHeapBlock (in brin_revmap.c:199, 239)
  - brinRevmapDesummarizeRange (in brin_revmap.c:327, 354)
  - REVMAP_CONTENT_SIZE (in brin_page.h:90)

## Notes and Other Information
- The array is designed to utilize all available space on the reverse map page for maximum efficiency
- Each element maps a specific heap block range to its corresponding BRIN summary tuple
- The reverse map is essential for BRIN index operations including tuple lookup and range queries
- The structure would ideally use a flexible array member but uses a workaround due to C language limitations
- This mapping allows BRIN to quickly find which summary tuple covers any given heap block
- The reverse map pages are separate from the main BRIN index pages and are managed independently
- Efficient reverse mapping is crucial for BRIN performance, especially during index scans