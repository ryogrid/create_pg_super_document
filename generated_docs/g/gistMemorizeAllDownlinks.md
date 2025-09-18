# gistMemorizeAllDownlinks

## Location
src/backend/access/gist/gistbuild.c: 1544 - 1564

## Overview
Scans all child page references (downlinks) on a GiST internal page and records their parent relationship in the parent map.

## Definition
```c
static void gistMemorizeAllDownlinks(GISTBuildState *buildstate, Buffer parentbuf)
```

## Detailed Description
This function iterates through all index tuples on a GiST internal page and extracts the block numbers of the child pages they point to. For each child page found, it calls gistMemorizeParent to record the parent-child relationship in the parent map hash table. The function ensures that the page is not a leaf page (using Assert) since only internal pages contain downlinks to other pages. This is essential for maintaining the complete hierarchical structure during GiST index construction.

## Parameters / Member Variables
- `buildstate`: Pointer to the GISTBuildState structure containing the parent map and other build state information
- `parentbuf`: Buffer containing the parent page whose downlinks will be processed

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
  - [BufferGetPage](../B/BufferGetPage.md)
  - GistPageIsLeaf
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - [PageGetItem](../P/PageGetItem.md)
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)
  - [gistMemorizeParent](gistMemorizeParent.md)
  - FirstOffsetNumber
  - ItemId
- Called from (representative examples):
  - gistbufferinginserttuples

## Notes and Other Information
- This is a static function, only accessible within the gistbuild.c file
- Contains an assertion to ensure the page is not a leaf page, as leaf pages don't have downlinks
- Processes all valid offset numbers from FirstOffsetNumber to MaxOffsetNumber
- Each index tuple's t_tid field contains the block number of the child page it points to
- Critical for building the complete parent-child mapping during buffering-based GiST construction
- The function assumes the page is properly formatted and all items are valid index tuples