# brinGetTupleForHeapBlock

## Location
src/backend/access/brin/brin_revmap.c: 194 - 322

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
  - revmap_get_blkno
  - ReadBuffer
  - LockBuffer
  - BufferGetBlockNumber
  - PageGetContents
  - HEAPBLK_TO_REVMAP_INDEX
  - ItemPointerIsValid
  - ItemPointerEquals
  - BRIN_IS_REGULAR_PAGE
  - PageGetMaxOffsetNumber
  - PageGetItemId
  - ItemIdIsUsed
  - PageGetItem
- Called from (representative examples):
  - brininsert
  - bringetbitmap
  - summarize_range
  - brinsummarize

## Notes and Other Information
- Returns NULL if no tuple is found for the given heap range
- The returned tuple points to shared buffer memory and must not be freed
- Callers should create a palloc'ed copy if they need to use the tuple after releasing the buffer lock
- Includes optimization for reusing pinned buffers across multiple calls
- Contains corruption detection logic that reports INDEX_CORRUPTED errors for infinite loops
- Handles concurrent desummarization by returning NULL when tuples are not found