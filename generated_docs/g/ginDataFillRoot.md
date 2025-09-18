# ginDataFillRoot

## Location
src/backend/access/gin/gindatapage.c: 1349 - 1369

## Overview
ginDataFillRoot fills a new root page with right bound values from left and right child pages in PostgreSQL's GIN (Generalized Inverted Index) data page management system.

## Definition


## Detailed Description
This function initializes a new root page by creating PostingItems for both left and right child pages and adding them to the root page. Each PostingItem contains the right bound key from the respective child page and the block number where that child page is located. This operation is typically performed during B-tree splits when a new root level needs to be created.

The function is designed to be callable from both normal B-tree operations and WAL (Write-Ahead Logging) recovery operations (ginxlog), which is why it doesn't rely heavily on the btree parameter for its core functionality.

## Parameters / Member Variables
- : GinBtree structure representing the GIN B-tree context (may be unused in some contexts)
- : The new root page to be filled
- : Block number of the left child page
- : The left child page
- : Block number of the right child page  
- : The right child page

## Dependencies
- Functions called/Symbols referenced:
  - GinDataPageGetRightBound
  - PostingItemSetBlockNumber
  - GinDataPageAddPostingItem
  - InvalidOffsetNumber (constant)
- Called from (representative examples):
  - ginPrepareDataScan
  - GinBtreeDataLeafInsertData (via function pointer)

## Notes and Other Information
- This is a public function (not static) and can be called from other modules
- The function creates two PostingItems, one for each child page
- Uses InvalidOffsetNumber when adding items, allowing the system to determine the appropriate insertion position
- Can be called during WAL recovery operations, making it important for crash recovery
- Located in src/backend/access/gin/gindatapage.c at lines 1349-1369
- The comment indicates it should not rely on the btree parameter since it's called from ginxlog