# ginScanBeginPostingTree

## Location
src/backend/access/gin/gindatapage.c: 1936 - 1947

## Overview
ginScanBeginPostingTree initiates a new scan operation on a GIN posting tree by setting up the B-tree structure and positioning at the leftmost leaf page for sequential access.

## Definition
```c
GinBtreeStack *ginScanBeginPostingTree(GinBtree btree, Relation index, BlockNumber rootBlkno)
```

## Detailed Description
ginScanBeginPostingTree prepares for a complete sequential scan of a GIN posting tree. The function:

1. Calls ginPrepareDataScan to initialize the GinBtree structure with posting tree-specific function pointers and configuration
2. Sets the fullScan flag to true, indicating this is a complete tree traversal rather than a targeted search
3. Uses ginFindLeafPage with searchMode=true and rootdescend=false to navigate to the leftmost leaf page of the posting tree
4. Returns a GinBtreeStack that represents the current scan position and path through the tree

This function is the entry point for reading all item pointers from a posting tree, typically used during index scans when retrieving all TIDs associated with a particular key value.

## Parameters / Member Variables
- `btree`: Pointer to GinBtreeData structure to be configured for the posting tree scan
- `index`: The GIN index relation containing the posting tree
- `rootBlkno`: Block number of the root page of the posting tree to be scanned

## Dependencies
- Functions called/Symbols referenced:
  - [ginPrepareDataScan](ginPrepareDataScan.md)
  - [ginFindLeafPage](ginFindLeafPage.md)
- Called from (representative examples):
  - [scanPostingTree](../s/scanPostingTree.md)
  - [startScanEntry](../s/startScanEntry.md)

## Notes and Other Information
- The function sets up for a left-to-right sequential scan of all leaf pages in the posting tree
- The fullScan=true setting enables optimizations specific to complete tree traversals
- The returned GinBtreeStack maintains the path from root to current leaf for efficient page navigation
- This function is typically called at the beginning of query execution when all TIDs for a key need to be retrieved
- The scan position can be advanced using other GIN scanning functions that work with the returned stack
- The function is essential for the GIN index's ability to efficiently retrieve all matching item pointers during query processing