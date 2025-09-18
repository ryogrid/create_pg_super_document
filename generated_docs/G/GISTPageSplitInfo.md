# GISTPageSplitInfo

## Location
src/include/access/gist_private.h: 423 - 475

## Overview
GISTPageSplitInfo represents information about a page split operation in a GiST index, containing the buffer and downlink tuple for each split page half.

## Definition


## Detailed Description
GISTPageSplitInfo is a structure that encapsulates the essential information about each page created during a GiST index page split operation. When a GiST page becomes too full to accommodate new tuples, it must be split into multiple pages. This structure tracks each resulting page half along with its corresponding downlink tuple.

The structure is designed to be used in lists, where each list element represents one of the pages created during the split. This allows the split operation to return comprehensive information about all the new pages that need to be properly linked into the index tree structure.

## Parameters / Member Variables
- : Buffer containing one of the split page halves, representing the actual page data
- : IndexTuple that serves as the downlink for this page half, used to maintain the tree structure by pointing from parent to child pages

## Dependencies
- Functions called/Symbols referenced:
  - Buffer (for buf field)
  - IndexTuple (for downlink field)
- Called from (representative examples):
  - gistplacetopage (creates and returns list of GISTPageSplitInfo structures)
  - gistfixsplit (processes split information during page split operations)
  - gistfinishsplit (completes split operations using split information)
  - gistbufferinginserttuples (handles split information during buffering builds)
  - gistFreeBuildBuffers (cleans up split information)
  - gistRelocateBuildBuffersOnSplit (relocates buffers based on split information)

## Notes and Other Information
- Returned as a List from gistplacetopage() function in the splitinfo parameter
- Used primarily during page split operations to coordinate the creation of new pages and their integration into the index tree
- Each split operation typically creates at least two GISTPageSplitInfo structures (for left and right halves)
- The downlink tuples are crucial for maintaining the hierarchical structure of the GiST index
- Memory for these structures is allocated using palloc() and should be freed appropriately
- Split information is processed in gistfinishsplit() to insert proper downlinks into parent pages