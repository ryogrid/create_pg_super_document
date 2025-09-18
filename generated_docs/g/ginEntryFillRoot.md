# ginEntryFillRoot

## Location
[src/backend/access/gin/ginentrypage.c:723-746](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginentrypage.c#L723-L746)

## Overview
Initializes a new root page for a GIN entry tree by inserting downlink tuples to the left and right child pages after a root split operation.

## Definition
void ginEntryFillRoot(GinBtree btree, Page root, BlockNumber lblkno, Page lpage, BlockNumber rblkno, Page rpage)

## Detailed Description
This function fills a newly created root page with two downlink tuples that point to the left and right child pages resulting from a root page split. It extracts the rightmost tuple from each child page to use as the separator key and creates interior tuples with the appropriate downlinks. The function is designed to be safe for use during WAL recovery operations and does not rely on the btree parameter for its core functionality.

The function performs two main operations:
1. Creates an interior tuple using the rightmost key from the left page and adds it to the root with a downlink to the left child
2. Creates an interior tuple using the rightmost key from the right page and adds it to the root with a downlink to the right child

## Parameters / Member Variables
- btree: GIN B-tree structure (not used in current implementation but kept for consistency)
- root: The new root page to be filled with downlink tuples
- lblkno: Block number of the left child page
- lpage: Left child page containing tuples
- rblkno: Block number of the right child page  
- rpage: Right child page containing tuples

## Dependencies
- Functions called/Symbols referenced:
  - [GinFormInteriorTuple](../G/GinFormInteriorTuple.md)
  - [getRightMostTuple](getRightMostTuple.md)
  - PageAddItem
  - IndexTupleSize
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [ginPrepareEntryScan](ginPrepareEntryScan.md) (via btree->fillRoot function pointer)
  - [GinBtreeDataLeafInsertData](../G/GinBtreeDataLeafInsertData.md) (via function pointer assignment)

## Notes and Other Information
- This function is also called from ginxlog during WAL recovery, so it must not depend on btree-specific state
- The function will throw an ERROR if either PageAddItem operation fails, indicating a serious page corruption issue
- Memory management is handled properly with pfree() calls for the temporary tuples
- The function assumes both child pages contain at least one tuple to extract the rightmost key from