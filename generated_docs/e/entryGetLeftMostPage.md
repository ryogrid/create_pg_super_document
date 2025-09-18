# entryGetLeftMostPage

## Location
src/backend/access/gin/ginentrypage.c: 446 - 458

## Overview
Returns the block number of the leftmost child page from a non-leaf GIN index page by extracting the downlink from the first index tuple.

## Definition


## Detailed Description
This function provides a simple and direct way to obtain the leftmost child page from a non-leaf page in a GIN index. It accesses the first index tuple on the page (at FirstOffsetNumber) and extracts its downlink, which points to the leftmost child page. This operation is commonly used during full index scans or when traversing to the leftmost branch of the index tree.

The function includes several assertions to ensure it operates on a valid non-leaf, non-data page that contains at least one entry. It serves as a callback function that can be registered in the GinBtree structure for operations that need to find the leftmost child.

## Parameters / Member Variables
- : GinBtree structure (currently unused in function body but required for callback interface consistency)
- : The non-leaf page from which to extract the leftmost child page block number

## Dependencies
- Functions called/Symbols referenced:
  - GinPageIsLeaf
  - GinPageIsData
  - PageGetMaxOffsetNumber
  - FirstOffsetNumber
  - PageGetItem
  - PageGetItemId
  - GinGetDownlink
- Called from (representative examples):
  - ginPrepareEntryScan

## Notes and Other Information
- This is a static function internal to the GIN entry page implementation
- The function assumes the input page is a non-leaf, non-data page with at least one entry (verified by assertions)
- Always returns the downlink from the first offset number on the page
- Used as a callback function in GinBtree operations, particularly for full scans
- Simple but critical function for navigating to the leftmost branch during index traversal
- Part of the GIN index's strategy for systematic page traversal during operations like full index scans