# ginInsertItemPointers

## Location
src/backend/access/gin/gindatapage.c: 1908 - 1935

## Overview
ginInsertItemPointers inserts an array of item pointers (TIDs) into an existing GIN posting tree, performing multiple tree scans as needed to accommodate all items.

## Definition
```c
void ginInsertItemPointers(Relation index, BlockNumber rootBlkno,
                          ItemPointerData *items, uint32 nitem,
                          GinStatsData *buildStats)
```

## Detailed Description
ginInsertItemPointers handles the insertion of multiple item pointers into a GIN posting tree. The function:

1. Sets up a GinBtreeData structure using ginPrepareDataScan for posting tree operations
2. Configures build-specific settings if buildStats is provided (during index construction)
3. Initializes insertion data structure with the items array and tracking counters
4. Iterates through all items, processing them in batches:
   - For each batch, finds the appropriate leaf page using ginFindLeafPage
   - Inserts items using ginInsertValue, which may handle multiple items per call
   - Continues until all items are processed

The function may execute multiple tree scans because items might need to be inserted into different leaf pages, or because page splits during insertion require finding new insertion points for remaining items.

## Parameters / Member Variables
- `index`: The GIN index relation containing the posting tree
- `rootBlkno`: Block number of the root page of the posting tree
- `items`: Array of ItemPointerData (TIDs) to be inserted
- `nitem`: Total number of items in the items array
- `buildStats`: Statistics collection structure for index builds (NULL during normal operations)

## Dependencies
- Functions called/Symbols referenced:
  - ginPrepareDataScan
  - ginFindLeafPage
  - ginInsertValue
- Called from (representative examples):
  - createPostingTree
  - addItemPointersToLeafTuple
  - ginEntryInsert

## Notes and Other Information
- The function comment notes that multiple tree scans are "very rare" but possible
- The curitem field in insertdata tracks progress through the items array
- Each iteration processes as many items as can fit in the target leaf page
- The function is essential for both regular insertions and posting tree creation
- Build statistics are updated internally by the called functions when buildStats is provided
- The function handles the complexity of posting tree insertion, including potential page splits and tree structure changes