# gistformdownlink

## Location
src/backend/access/gist/gist.c: 1135 - 1194

## Overview
Creates a downlink index tuple that represents all entries on a given page, used when inserting references to child pages in internal nodes of the GiST tree.

## Definition


## Detailed Description
 constructs a downlink tuple that will be inserted into a parent page to reference a child page. The function works by:

1. **Union Computation**: Iterates through all tuples on the target page and computes their union using . This creates a bounding key that covers all entries on the child page.

2. **Empty Page Handling**: For completely empty pages, constructs a downlink by copying the original downlink from the parent page. This ensures the downlink is consistent with the parent's constraints while potentially being suboptimal for query performance.

3. **Tuple Finalization**: Sets the block number to point to the target buffer and marks the tuple as valid.

The union computation is essential for maintaining the GiST tree property that parent keys properly bound their children. When pages are split, new downlinks must be created for the resulting pages.

## Parameters / Member Variables
- : The GiST index relation
- : Buffer containing the page for which to create a downlink
- : GiST-specific state information including operator classes
- : Insertion stack used to locate parent information when needed for empty pages
- : Boolean indicating whether this is called during index build

## Dependencies
- Functions called/Symbols referenced:
  - gistgetadjusted
  - [gistFindCorrectParent](gistFindCorrectParent.md)
  - [BufferGetPage](../B/BufferGetPage.md)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - [PageGetItem](../P/PageGetItem.md)
  - [CopyIndexTuple](../C/CopyIndexTuple.md)
  - [ItemPointerSetBlockNumber](../I/ItemPointerSetBlockNumber.md)
  - GistTupleSetValid
  - [LockBuffer](../L/LockBuffer.md)
- Called from (representative examples):
  - [gistfixsplit](gistfixsplit.md)

## Notes and Other Information
- The function handles the special case of empty pages by reusing the parent's downlink, which is suboptimal but ensures correctness
- The union computation ensures that the resulting downlink properly bounds all entries on the child page
- Proper locking is used when accessing parent page information for empty page handling
- The resulting tuple has its block pointer set to reference the child page and is marked as valid
- This function is critical for maintaining the bounding property of GiST trees after page splits
- The choice to reuse parent downlinks for empty pages prioritizes correctness over optimality