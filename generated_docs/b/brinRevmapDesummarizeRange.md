# brinRevmapDesummarizeRange

## Location
src/backend/access/brin/brin_revmap.c: 323 - 441

## Overview
Deletes an index tuple from a BRIN index, marking a page range as unsummarized and removing its summary information from both the reverse map and the regular index pages.

## Definition


## Detailed Description
This function removes the summary tuple for a given heap block range by:
1. Initializing the reverse map and locating the relevant revmap page
2. Obtaining an exclusive lock on the revmap page for the specified heap block
3. Reading the ItemPointer from the revmap that points to the actual summary tuple
4. Locating and locking the regular index page containing the summary tuple
5. Validating the tuple exists and the pages are in the expected state
6. Removing the tuple from both the regular page and clearing the revmap entry
7. Recording the operation in WAL if necessary

The function handles various edge cases including missing tuples, concurrent page type changes, and leftover placeholder tuples from crashed operations. It operates within a critical section to ensure atomicity.

## Parameters / Member Variables
- : The BRIN index relation from which to remove the summary tuple
- : The heap block number whose range should be desummarized

## Dependencies
- Functions called/Symbols referenced:
  - brinRevmapInitialize
  - revmap_get_blkno
  - BlockNumberIsValid
  - brinRevmapTerminate
  - brinLockRevmapPageForUpdate
  - HEAPBLK_TO_REVMAP_INDEX
  - PageGetContents
  - ItemPointerIsValid
  - ReadBuffer
  - ItemPointerGetBlockNumber
  - BRIN_IS_REGULAR_PAGE
  - PageGetMaxOffsetNumber
  - PageGetItemId
  - ItemIdIsUsed
  - brinSetHeapBlockItemptr
  - PageIndexTupleDeleteNoCompact
  - MarkBufferDirty
  - XLogBeginInsert
  - XLogRegisterData
  - XLogRegisterBuffer
  - XLogInsert
- Called from (representative examples):
  - brin_desummarize_range

## Notes and Other Information
- Requires ShareUpdateExclusiveLock on the index to prevent concurrent summarization
- Returns false if the caller should retry the operation (due to concurrent page changes)
- Returns true if the operation completed successfully or if the range was already unsummarized
- Handles placeholder tuples from aborted summarization operations by removing them silently
- Records the desummarization in WAL for crash recovery when WAL logging is enabled
- Uses critical sections to ensure the operation is atomic across both revmap and regular pages
- Validates index consistency and reports INDEX_CORRUPTED errors for invalid states