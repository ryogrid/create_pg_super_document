# spgRedoVacuumLeaf

## Location
[src/backend/access/spgist/spgxlog.c:751-833](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgxlog.c#L751-L833)

## Overview
Replays a vacuum leaf operation from the WAL for SP-GiST indexes, reconstructing the cleanup and compaction of dead tuples on leaf pages during crash recovery.

## Definition

```c
static void
spgRedoVacuumLeaf(XLogReaderState *record)
```
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
  - [fillFakeState](../f/fillFakeState.md)
  - XLogReadBufferForRedo
  - [spgPageIndexMultiDelete](spgPageIndexMultiDelete.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - [PageGetItem](../P/PageGetItem.md)
  - SGLT_SET_NEXTOFFSET
  - [PageSetLSN](../P/PageSetLSN.md)
  - MarkBufferDirty
  - [BufferGetPage](../B/BufferGetPage.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
- Called from:
  - [spg_redo](spg_redo.md) (main SP-GiST redo dispatcher)

## Notes and Other Information
- Processes multiple types of tuple state changes: DEAD, PLACEHOLDER, and tuple movement
- Implements tuple movement by swapping ItemId entries to compact page layout
- Maintains chain relationships between tuples using SGLT_SET_NEXTOFFSET
- Follows the same logic as the original vacuumLeafPage() function for consistency
- Uses spgPageIndexMultiDelete for batch tuple state changes to improve efficiency
- Part of the SP-GiST index WAL recovery subsystem located in src/backend/access/spgist/spgxlog.c:751-833
- Critical for maintaining space efficiency and preventing page fragmentation during recovery