# dataExecPlaceToPage

## Location
src/backend/access/gin/gindatapage.c: 1231 - 1251

## Overview
dataExecPlaceToPage serves as a dispatcher function that executes data insertion on a GIN posting-tree data page by delegating to the appropriate specialized function based on whether the page is a leaf or internal node.

## Definition
```c
static void dataExecPlaceToPage(GinBtree btree, Buffer buf, GinBtreeStack *stack,
                               void *insertdata, BlockNumber updateblkno,
                               void *ptp_workspace)
```

## Detailed Description
This function acts as a routing mechanism for data insertion execution in GIN posting-tree data pages. It operates within a critical section after the corresponding dataBeginPlaceToPage function has determined that the insertion will fit. The function examines the page type using GinPageIsLeaf and delegates the actual insertion work to either dataExecPlaceToPageLeaf for leaf pages or dataExecPlaceToPageInternal for internal pages.

The function is designed to be called with XLOG record creation already initialized and the target buffer pre-registered in slot 0 for WAL logging purposes. It maintains the separation between leaf and internal page handling while providing a unified interface for the insertion execution phase.

## Parameters / Member Variables
- `btree`: GIN B-tree structure containing tree metadata and configuration
- `buf`: Buffer containing the target data page for insertion (registered in slot 0)
- `stack`: GIN B-tree stack indicating the insertion position and path
- `insertdata`: Pointer to the data item to be inserted
- `updateblkno`: Block number for downlink updates (used only for internal pages)
- `ptp_workspace`: Workspace information passed from the begin phase

## Dependencies
- Functions called/Symbols referenced:
  - GinPageIsLeaf
  - [dataExecPlaceToPageLeaf](dataExecPlaceToPageLeaf.md)
  - [dataExecPlaceToPageInternal](dataExecPlaceToPageInternal.md)
  - [GinBtree](../G/GinBtree.md) (struct)
  - [GinBtreeStack](../G/GinBtreeStack.md) (struct)
- Called from:
  - [ginPrepareDataScan](../g/ginPrepareDataScan.md)

## Notes and Other Information
- This function serves as a type dispatcher for the execution phase of insertion operations
- It operates within a critical section and assumes XLOG record creation is already initialized
- The updateblkno parameter is only relevant for internal pages and is passed through accordingly
- Both leaf and internal page handlers follow the same interface contract for consistency
- The function is the execution counterpart to dataBeginPlaceToPage, completing the two-phase insertion process
- The target buffer must be pre-registered in slot 0 for proper WAL logging by the specialized handlers