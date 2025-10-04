# spgRedoVacuumRedirect

## Location
[src/backend/access/spgist/spgxlog.c:860-934](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgxlog.c#L860-L934)

## Overview
Replays a vacuum redirect operation from the WAL for SP-GiST indexes, reconstructing the cleanup of redirect tuples and conversion to placeholders during crash recovery.

## Definition

```c
static void
spgRedoVacuumRedirect(XLogReaderState *record)
```
## Detailed Description
This function handles the replay of SP-GiST vacuum redirect operations, which clean up redirect tuples that are no longer needed. Redirect tuples are temporary placeholders created during page splits to maintain consistency, but they need to be cleaned up eventually. The function performs several key operations:

1. Resolves potential Hot Standby conflicts if running in standby mode
2. Converts redirect tuples to plain placeholder tuples by changing their state
3. Updates page opaque data counters (nRedirection and nPlaceholder)
4. Removes trailing placeholder tuples at the end of the page for space reclamation
5. Maintains proper page statistics and layout

The operation ensures that redirect cleanup during recovery maintains the same consistency and Hot Standby compatibility as during normal operation.

## Parameters / Member Variables
- `*record`: XLogReaderState pointer containing the WAL record with vacuum redirect operation details
## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - [XLogRecGetBlockTag](../X/XLogRecGetBlockTag.md)
  - [ResolveRecoveryConflictWithSnapshot](../R/ResolveRecoveryConflictWithSnapshot.md)
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md)
  - SpGistPageGetOpaque
  - [PageGetItem](../P/PageGetItem.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - [ItemPointerSetInvalid](../I/ItemPointerSetInvalid.md)
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - [PageIndexMultiDelete](../P/PageIndexMultiDelete.md)
  - [BufferGetPage](../B/BufferGetPage.md)
  - [PageSetLSN](../P/PageSetLSN.md)
  - [MarkBufferDirty](../M/MarkBufferDirty.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
- Called from:
  - [spg_redo](spg_redo.md) (main SP-GiST redo dispatcher)

## Notes and Other Information
- Handles Hot Standby conflicts by resolving snapshot conflicts before processing
- Converts SPGIST_REDIRECT tuples to SPGIST_PLACEHOLDER state
- Updates page opaque structure counters to maintain accurate statistics
- Performs trailing placeholder cleanup for space efficiency
- Uses palloc/pfree for temporary memory allocation during cleanup
- Ensures proper ordering with PageIndexMultiDelete for batch deletions
- Part of the SP-GiST index WAL recovery subsystem located in src/backend/access/spgist/spgxlog.c:860-934
- Critical for maintaining SP-GiST index consistency and preventing redirect tuple accumulation

## Simplified Source

```c
static void spgRedoVacuumRedirect(XLogReaderState *record) {
    // Extract WAL record data
    spgxlogVacuumRedirect *xldata = (spgxlogVacuumRedirect *) XLogRecGetData(record);
    OffsetNumber *itemToPlaceholder = xldata->offsets;

    // Handle Hot Standby conflicts if running in standby mode
    if (InHotStandby) {
        RelFileLocator locator;
        XLogRecGetBlockTag(record, 0, &locator, NULL, NULL);
        ResolveRecoveryConflictWithSnapshot(xldata->snapshotConflictHorizon,
                                           xldata->isCatalogRel, locator);
    }

    Buffer buffer;
    if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO) {
        Page page = BufferGetPage(buffer);
        SpGistPageOpaque opaque = SpGistPageGetOpaque(page);

        // Step 1: Convert redirect tuples to plain placeholders
        for (int i = 0; i < xldata->nToPlaceholder; i++) {
            SpGistDeadTuple deadTuple = (SpGistDeadTuple) PageGetItem(page,
                                        PageGetItemId(page, itemToPlaceholder[i]));

            // Change state from REDIRECT to PLACEHOLDER
            deadTuple->tupstate = SPGIST_PLACEHOLDER;
            ItemPointerSetInvalid(&deadTuple->pointer);
        }

        // Step 2: Update page opaque counters
        opaque->nRedirection -= xldata->nToPlaceholder;
        opaque->nPlaceholder += xldata->nToPlaceholder;

        // Step 3: Remove trailing placeholder tuples for space reclamation
        if (xldata->firstPlaceholder != InvalidOffsetNumber) {
            int maxOffset = PageGetMaxOffsetNumber(page);
            int numToDelete = maxOffset - xldata->firstPlaceholder + 1;

            // Build array of offsets to delete (trailing placeholders)
            OffsetNumber *toDelete = palloc(sizeof(OffsetNumber) * maxOffset);
            for (int i = xldata->firstPlaceholder; i <= maxOffset; i++) {
                toDelete[i - xldata->firstPlaceholder] = i;
            }

            // Update placeholder count and delete trailing tuples
            opaque->nPlaceholder -= numToDelete;
            PageIndexMultiDelete(page, toDelete, numToDelete);

            pfree(toDelete);
        }

        PageSetLSN(page, record->EndRecPtr);
        MarkBufferDirty(buffer);
    }
    UnlockReleaseBuffer(buffer);
}
```