# spgRedoVacuumLeaf

## Location
src/backend/access/spgist/spgxlog.c: 751 - 833

## Overview
Replays a vacuum leaf operation from the WAL for SP-GiST indexes, reconstructing the cleanup and compaction of dead tuples on leaf pages during crash recovery.

## Definition


## Detailed Description
This function handles the replay of SP-GiST leaf page vacuum operations, which clean up dead tuples and reorganize the page layout for better space utilization. The vacuum process involves several distinct operations:

1. Marking certain tuples as DEAD (completely removed)
2. Converting some tuples to PLACEHOLDER status (preserving space but removing content)
3. Moving tuples to compact the page layout by swapping ItemId entries
4. Updating chain pointers for tuples that maintain linked list relationships
5. Cleaning up moved tuple locations by marking them as placeholders

The function processes arrays of offset numbers for each operation type and applies them in the correct sequence to maintain page consistency.

## Parameters / Member Variables
- : XLogReaderState pointer containing the WAL record with vacuum leaf operation details

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - fillFakeState
  - XLogReadBufferForRedo
  - spgPageIndexMultiDelete
  - PageGetItemId
  - PageGetItem
  - SGLT_SET_NEXTOFFSET
  - PageSetLSN
  - MarkBufferDirty
  - BufferGetPage
  - UnlockReleaseBuffer
- Called from:
  - spg_redo (main SP-GiST redo dispatcher)

## Notes and Other Information
- Processes multiple types of tuple state changes: DEAD, PLACEHOLDER, and tuple movement
- Implements tuple movement by swapping ItemId entries to compact page layout
- Maintains chain relationships between tuples using SGLT_SET_NEXTOFFSET
- Follows the same logic as the original vacuumLeafPage() function for consistency
- Uses spgPageIndexMultiDelete for batch tuple state changes to improve efficiency
- Part of the SP-GiST index WAL recovery subsystem located in src/backend/access/spgist/spgxlog.c:751-833
- Critical for maintaining space efficiency and preventing page fragmentation during recovery