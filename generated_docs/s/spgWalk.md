# spgWalk

## Location
[src/backend/access/spgist/spgscan.c:817-930](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgscan.c#L817-L930)

## Overview
Core function that walks the SP-GiST tree and reports all tuples passing scan qualifiers to a result storage subroutine.

## Definition
static void spgWalk(Relation index, SpGistScanOpaque so, bool scanWholeIndex, storeRes_func storeRes)

## Detailed Description
This function is the main tree traversal engine for SP-GiST index scans. It processes search items from a priority queue, handling both leaf and inner nodes. For leaf items (heap tuples), it directly stores results when doing ordered searches. For tree nodes, it reads the appropriate pages, determines if they are leaf or inner pages, and processes them accordingly. On leaf pages, it examines tuple chains or all tuples (for root), while on inner pages it processes inner tuples to generate new search items. The function supports both complete index scans and partial scans that stop after finding results on the next page boundary.

## Parameters / Member Variables
- : Relation representing the SP-GiST index being scanned
- : SpGistScanOpaque structure containing scan state, queues, and context information
- : Boolean indicating whether to scan the entire index or stop at next page boundary after finding results
- : Function pointer for storing scan results

## Dependencies
- Functions called/Symbols referenced:
  - [spgGetNextQueueItem](spgGetNextQueueItem.md)
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)
  - [ItemPointerGetOffsetNumber](../I/ItemPointerGetOffsetNumber.md)
  - [ReadBuffer](../R/ReadBuffer.md)
  - [LockBuffer](../L/LockBuffer.md)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
  - [BufferGetPage](../B/BufferGetPage.md)
  - SpGistPageStoresNulls
  - SpGistPageIsLeaf
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - SpGistBlockIsRoot
  - [spgTestLeafTuple](spgTestLeafTuple.md)
  - [PageGetItem](../P/PageGetItem.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - [spgInnerTest](spgInnerTest.md)
  - [spgFreeSearchItem](spgFreeSearchItem.md)
  - [MemoryContextReset](../M/MemoryContextReset.md)
- Called from (representative examples):
  - [spggetbitmap](spggetbitmap.md)
  - [spggettuple](spggettuple.md)

## Notes and Other Information
- This is a static function internal to spgscan.c
- Uses a priority queue (pairing heap) to maintain search items in order
- Handles page locking and buffer management efficiently by reusing buffers for the same page
- Supports both ordered and unordered scans through the scanWholeIndex parameter
- Handles tuple state transitions including redirects and dead tuples
- Includes interrupt checking to prevent infinite loops
- Located at src/backend/access/spgist/spgscan.c:817-930