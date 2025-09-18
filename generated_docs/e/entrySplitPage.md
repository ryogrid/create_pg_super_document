# entrySplitPage

## Location
[src/backend/access/gin/ginentrypage.c:602-701](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginentrypage.c#L602-L701)

## Overview
Performs a page split operation for GIN entry pages when insufficient space exists for new tuple insertion, creating two balanced temporary pages.

## Definition
```c
static void entrySplitPage(GinBtree btree, Buffer origbuf,
                          GinBtreeStack *stack,
                          GinBtreeEntryInsertData *insertData,
                          BlockNumber updateblkno,
                          Page *newlpage, Page *newrpage)
```

## Detailed Description
entrySplitPage implements the page splitting logic for GIN entry pages when a new tuple cannot fit on the current page. The function follows a sophisticated multi-step process:

1. **Page Preparation**: Calls entryPreparePage to handle any necessary cleanup operations on a temporary copy of the original page.

2. **Tuple Collection**: Gathers all existing tuples from the original page plus the new tuple being inserted into a temporary workspace (tupstore), maintaining proper alignment and calculating total size requirements.

3. **Split Point Calculation**: Determines the optimal split point by attempting to equalize the total data size (not tuple count) between the left and right pages, targeting approximately 50% of total size on each page.

4. **Page Creation**: Initializes two new temporary pages (left and right) with the same flags as the original page, then distributes tuples across them based on the calculated split point.

5. **Tuple Distribution**: Copies tuples from the temporary workspace to the appropriate page (left or right) based on the split decision, ensuring proper page structure and error handling.

The function ensures balanced page utilization while preserving the original buffer contents.

## Parameters / Member Variables
- `btree`: GinBtree structure containing B-tree context and index relation information
- `origbuf`: Buffer containing the original page that needs to be split (left unmodified)
- `stack`: GinBtreeStack indicating the insertion position within the original page
- `insertData`: GinBtreeEntryInsertData containing the new tuple and insertion metadata
- `updateblkno`: Block number for updating downlinks in internal nodes (when child splits occur)
- `newlpage`: Output pointer for the newly created left page image
- `newrpage`: Output pointer for the newly created right page image

## Dependencies
- Functions called/Symbols referenced:
  - [entryPreparePage](entryPreparePage.md)
  - PageGetTempPageCopy
  - [PageGetPageSize](../P/PageGetPageSize.md)  
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - [PageGetItem](../P/PageGetItem.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - [GinInitPage](../G/GinInitPage.md)
  - GinPageGetOpaque
  - PageAddItem
  - IndexTupleSize
  - MAXALIGN
  - RelationGetRelationName (for error reporting)
- Called from (representative examples):
  - [entryBeginPlaceToPage](entryBeginPlaceToPage.md)

## Notes and Other Information
- This is a static function used internally within the GIN entry page management system
- The original buffer remains untouched - all operations work on temporary page copies
- Split logic prioritizes data size balance over tuple count balance for optimal space utilization
- Uses a PGAlignedBlock workspace that can accommodate up to 2 pages worth of tuples
- The function handles edge cases for insertions at the beginning, middle, or end of the page
- Error handling ensures insertion failures are reported with the relation name for debugging
- Part of the GIN index maintenance system ensuring balanced tree structure as data grows