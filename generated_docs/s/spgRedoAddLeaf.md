# spgRedoAddLeaf

## Location
[src/backend/access/spgist/spgxlog.c:74-170](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgxlog.c#L74-L170)

## Overview
Replays the addition of a leaf tuple to an SP-GiST index during WAL recovery, handling both new page creation and updates to existing pages with proper parent link maintenance.

## Definition
```c
static void spgRedoAddLeaf(XLogReaderState *record)
```

## Detailed Description
This function handles the WAL replay of SP-GiST leaf tuple addition operations. It reconstructs the state of SP-GiST index pages by:

1. **Parsing WAL record**: Extracts the spgxlogAddLeaf structure and leaf tuple data from the WAL record
2. **Page initialization**: If this is a new page (xldata->newPage), initializes it as a SP-GiST leaf page with appropriate flags
3. **Tuple insertion**: Adds the leaf tuple using one of two methods:
   - Normal addition via addOrReplaceTuple() for new tuples
   - Direct replacement for DEAD tuples using PageIndexTupleDelete() and PageAddItem()
4. **Chain link updates**: Updates the chain links between leaf tuples when inserting into an existing chain
5. **Parent downlink updates**: Updates the parent inner tuple's downlink to point to the new leaf location

The function carefully handles the unaligned leaf tuple data by copying it to a properly aligned structure before accessing its fields.

## Parameters / Member Variables
- `record`: XLogReaderState containing the WAL record data for the add leaf operation

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData (extract WAL record data)
  - memcpy (copy unaligned tuple header)
  - XLogInitBufferForRedo (initialize new buffer)
  - [SpGistInitBuffer](../S/SpGistInitBuffer.md) (initialize SP-GiST page)
  - XLogReadBufferForRedo (read existing buffer)
  - [BufferGetPage](../B/BufferGetPage.md) (get page from buffer)
  - [addOrReplaceTuple](../a/addOrReplaceTuple.md) (add or replace tuple on page)
  - [PageGetItem](../P/PageGetItem.md), PageGetItemId (page item access)
  - [PageIndexTupleDelete](../P/PageIndexTupleDelete.md), PageAddItem (page modification)
  - [XLogRecGetBlockTag](../X/XLogRecGetBlockTag.md) (get block information)
  - [spgUpdateNodeLink](spgUpdateNodeLink.md) (update parent node links)
  - [PageSetLSN](../P/PageSetLSN.md), MarkBufferDirty (page finalization)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md) (resource cleanup)
  - SGLT_GET_NEXTOFFSET, SGLT_SET_NEXTOFFSET (leaf tuple macros)
- Called from (representative examples):
  - [spg_redo](spg_redo.md) (main SP-GiST WAL redo dispatcher)

## Notes and Other Information
- This is a static function used only within the SP-GiST WAL replay module (spgxlog.c)
- The function handles both new page creation (xldata->newPage flag) and updates to existing pages  
- Special handling for unaligned tuple data by copying to aligned structure before field access
- Maintains proper chain links between leaf tuples using SGLT_GET_NEXTOFFSET and SGLT_SET_NEXTOFFSET macros
- Updates parent downlinks when necessary to maintain index consistency
- Uses two-phase approach: first update leaf page, then update parent page (safe during WAL replay)
- Supports both null-storing and regular leaf pages via SPGIST_NULLS flag
- Error checking ensures tuple addition succeeds or aborts with elog(ERROR)