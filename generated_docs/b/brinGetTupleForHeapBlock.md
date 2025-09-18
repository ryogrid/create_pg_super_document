# brinGetTupleForHeapBlock

## Location
[src/backend/access/brin/brin_revmap.c:194-322](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_revmap.c#L194-L322)

## Overview
Fetches the BrinTuple for a given heap block from a BRIN (Block Range Index) reverse map, returning the tuple that summarizes the range containing the specified heap block.

## Definition


## Detailed Description
This function retrieves the BRIN summary tuple for a specified heap block by:
1. Normalizing the heap block number to the first page in its range
2. Computing the reverse map page number using revmap_get_blkno
3. Reading the reverse map page to get the ItemPointer to the actual tuple
4. Following the pointer to fetch the BrinTuple from the index page
5. Performing validation checks to ensure data consistency

The function handles concurrent operations gracefully by detecting when range maps are updated or tuples are moved/removed during the operation. It includes an infinite loop protection mechanism that detects corrupted index conditions.

## Parameters / Member Variables
- : The BRIN reverse map structure containing index metadata
- : The heap block number for which to fetch the summary tuple
- : Pointer to buffer that will contain the fetched tuple (input/output parameter)
- : Returns the offset number of the tuple within its page
- : Returns the size of the tuple (optional, can be NULL)
- : Buffer lock mode to apply when reading the tuple page

## Dependencies
- Functions called/Symbols referenced:
  - [revmap_get_blkno](../r/revmap_get_blkno.md)
  - [ReadBuffer](../R/ReadBuffer.md)
  - [LockBuffer](../L/LockBuffer.md)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
  - [PageGetContents](../P/PageGetContents.md)
  - HEAPBLK_TO_REVMAP_INDEX
  - [ItemPointerIsValid](../I/ItemPointerIsValid.md)
  - [ItemPointerEquals](../I/ItemPointerEquals.md)
  - BRIN_IS_REGULAR_PAGE
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - ItemIdIsUsed
  - [PageGetItem](../P/PageGetItem.md)
- Called from (representative examples):
  - [brininsert](brininsert.md)
  - [bringetbitmap](bringetbitmap.md)
  - [summarize_range](../s/summarize_range.md)
  - [brinsummarize](brinsummarize.md)

## Notes and Other Information
- Returns NULL if no tuple is found for the given heap range
- The returned tuple points to shared buffer memory and must not be freed
- Callers should create a palloc'ed copy if they need to use the tuple after releasing the buffer lock
- Includes optimization for reusing pinned buffers across multiple calls
- Contains corruption detection logic that reports INDEX_CORRUPTED errors for infinite loops
- Handles concurrent desummarization by returning NULL when tuples are not found