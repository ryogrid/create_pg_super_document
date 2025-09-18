# dataFindChildPtr

## Location
[src/backend/access/gin/gindatapage.c:319-363](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/gindatapage.c#L319-L363)

## Overview
dataFindChildPtr locates a specific child pointer on a non-leaf GIN data page by searching for the PostingItem that points to a given block number.

## Definition
static OffsetNumber dataFindChildPtr(GinBtree btree, Page page, BlockNumber blkno, OffsetNumber storedOff)

## Detailed Description
This function searches for a child pointer on a GIN B-tree data page (non-leaf) that points to a specific block number. It implements an optimization strategy by first checking a stored offset position, then searching to the right (assuming the pointer moved due to insertions), and finally performing a full scan if necessary. The function is essential for B-tree navigation during GIN index operations.

The search strategy is optimized for the common case where pages grow through insertions rather than deletions, so it searches forward from the stored offset first before falling back to a complete scan.

## Parameters / Member Variables
- btree: GinBtree structure containing B-tree context information
- page: The non-leaf data page to search within
- blkno: The target block number to find a pointer for
- storedOff: Previously known offset position as a starting hint for the search

## Dependencies
- Functions called/Symbols referenced:
  - GinPageGetOpaque
  - GinPageIsLeaf  
  - GinPageIsData
  - GinDataPageGetPostingItem
  - PostingItemGetBlockNumber
  - FirstOffsetNumber
  - InvalidOffsetNumber
- Called from (representative examples):
  - [ginPrepareDataScan](../g/ginPrepareDataScan.md)

## Notes and Other Information
- Only operates on non-leaf GIN data pages (verified by assertions)
- Uses a three-phase search strategy: check stored offset, search right, then full scan
- Returns InvalidOffsetNumber if the target block number is not found
- Optimized for insertion-heavy workloads where pointers typically move to the right
- Part of the GIN (Generalized Inverted Index) access method implementation