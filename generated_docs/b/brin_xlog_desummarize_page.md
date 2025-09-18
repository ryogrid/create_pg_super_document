# brin_xlog_desummarize_page

## Location
[src/backend/access/brin/brin_xlog.c:269-308](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_xlog.c#L269-L308)

## Overview
Replays a BRIN desummarization operation during WAL recovery, updating the revmap and removing tuple data from summary pages.

## Definition


## Detailed Description
This function handles the replay of a BRIN desummarization operation during crash recovery. Desummarization occurs when a BRIN index needs to invalidate summary information for a range of heap blocks, typically due to significant data changes that make the existing summary inaccurate. The function:

1. Extracts the xl_brin_desummarize record from the WAL entry
2. Updates the revmap by setting the corresponding entry to invalid, effectively removing the mapping from heap blocks to summary pages
3. Removes the leftover summary tuple from the regular BRIN page using PageIndexTupleDeleteNoCompact
4. Sets appropriate LSNs and marks both buffers as dirty

This operation ensures that the BRIN index correctly reflects the invalidated state of summary information during recovery.

## Parameters / Member Variables
- : XLogReaderState pointer containing the WAL record being replayed, including the desummarization data and references to the affected revmap and regular page blocks

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData: Extract record data from WAL
  - XLogReadBufferForRedo: Read and prepare buffers for redo operation
  - [ItemPointerSetInvalid](../I/ItemPointerSetInvalid.md): Set item pointer to invalid state
  - [brinSetHeapBlockItemptr](brinSetHeapBlockItemptr.md): Set heap block item pointer in revmap
  - [BufferGetPage](../B/BufferGetPage.md): Get page from buffer
  - [PageIndexTupleDeleteNoCompact](../P/PageIndexTupleDeleteNoCompact.md): Delete tuple from page without compacting
  - [PageSetLSN](../P/PageSetLSN.md): Set LSN on page
  - MarkBufferDirty: Mark buffer as modified
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md): Release buffer locks
- Called from (representative examples):
  - [brin_redo](brin_redo.md): Main BRIN WAL replay dispatcher function

## Notes and Other Information
- This is a static function used only within the BRIN WAL replay subsystem
- The function operates on two buffers: the revmap page (block 0) and the regular summary page (block 1)
- Uses PageIndexTupleDeleteNoCompact instead of compact deletion to maintain page structure during recovery
- Part of PostgreSQL's crash recovery mechanism for BRIN indexes
- The desummarization process effectively invalidates summary data, which may trigger re-summarization during subsequent index operations