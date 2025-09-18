# dataPlaceToPageLeafSplit

## Location
[src/backend/access/gin/gindatapage.c:1034-1118](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/gindatapage.c#L1034-L1118)

## Overview
dataPlaceToPageLeafSplit reconstructs a disassembled GIN data leaf page into two separate pages when a page split is required, distributing segments between left and right pages according to predefined boundaries.

## Definition


## Detailed Description
This function handles the physical reconstruction of a GIN data leaf page that needs to be split into two pages due to space constraints. Unlike the single-page recompression functions, this operates on temporary in-memory copies of the new left and right pages rather than modifying the original page directly.

The function first initializes both target pages with appropriate GIN page headers, setting them as compressed data leaf pages. It then distributes the segments from the disassembledLeaf structure between the two pages based on the split point stored in leaf->lastleft. Segments up to and including lastleft go to the left page, while remaining segments go to the right page.

For each page, the function iterates through the assigned segments, copying non-deleted segments to the appropriate memory location and tracking the total size. Deleted segments are skipped entirely and contribute no data to either page. The function sets the data size and right boundary for each page, ensuring proper page metadata is established for both resulting pages.

The split operation preserves the logical ordering of segments while physically distributing them across two pages, maintaining the integrity of the GIN index structure.

## Parameters / Member Variables
- : Pointer to disassembledLeaf structure containing segments and split information
- : Right boundary item pointer for the left page
- : Right boundary item pointer for the right page  
- : Target memory location for the reconstructed left page
- : Target memory location for the reconstructed right page

## Dependencies
- Functions called/Symbols referenced:
  - [GinInitPage](../G/GinInitPage.md)
  - GinDataLeafPageGetPostingList
  - GinDataPageSetDataSize
  - GinDataPageGetRightBound
  - SizeOfGinPostingList
  - [dlist_next_node](dlist_next_node.md)
  - [dlist_head_node](dlist_head_node.md)
  - [dlist_has_next](dlist_has_next.md)
  - dlist_container
  - memcpy
  - Assert
  - leafSegmentInfo (structure type)
  - [dlist_node](dlist_node.md) (structure type)
  - [ItemPointerData](../I/ItemPointerData.md) (structure type)
  - GIN_DATA (constant)
  - GIN_LEAF (constant)
  - GIN_COMPRESSED (constant)
  - GIN_SEGMENT_DELETE (constant)
- Called from (representative examples):
  - [dataBeginPlaceToPageLeaf](dataBeginPlaceToPageLeaf.md)

## Notes and Other Information
- Operates on temporary in-memory page copies rather than modifying original pages
- Both pages are initialized as compressed data leaf pages
- Split point is determined by the leaf->lastleft field in the disassembledLeaf structure
- Could be optimized to skip copying unmodified portions of the left page (noted in TODO comment)
- Validates that calculated sizes match the expected lsize and rsize from the disassembledLeaf
- Sets appropriate right boundary item pointers for both pages
- Maintains segment ordering across the split operation
- Handles segment deletion by skipping those segments entirely during reconstruction