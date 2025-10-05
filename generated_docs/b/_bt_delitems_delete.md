# _bt_delitems_delete

## Location
[src/backend/access/nbtree/nbtpage.c:1284-1404](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtpage.c#L1284-L1404)

## Overview
Deletes and updates items on a btree leaf page during single-page cleanup operations, handling both complete item deletions and partial updates to posting list tuples by removing specific TIDs.

## Definition

```c
static void
_bt_delitems_delete(Relation rel, Buffer buf,
					TransactionId snapshotConflictHorizon, bool isCatalogRel,
					OffsetNumber *deletable, int ndeletable,
					BTVacuumPosting *updatable, int nupdatable)
```
## Detailed Description
This function performs deletion and update operations on a B-tree leaf page during single-page cleanup. It handles two types of operations:

1. **Complete item deletion**: Removes entire index tuples from the page
2. **Partial posting list updates**: Updates existing posting list items by removing specific heap TIDs while preserving others

The function is nearly identical to  in terms of page modifications, but differs in that it:
- Uses its own  and  parameters for recovery conflict generation
- Does NOT clear the page's VACUUM cycle ID (only  controls vacuum cycle IDs)

The function ensures WAL logging consistency and handles both deletions and updates atomically within a critical section.

## Parameters / Member Variables
- `rel`: The btree index relation being modified
- `buf`: Buffer containing the leaf page to modify (must be pinned and write-locked by caller)
- `snapshotConflictHorizon`: Transaction ID for generating recovery conflicts during WAL replay
- `isCatalogRel`: Boolean indicating if this is a catalog relation (affects conflict handling)
- `*deletable`: Array of offset numbers for items to be completely deleted (must be sorted ascending)
- `ndeletable`: Number of items in the deletable array
- `*updatable`: Array of BTVacuumPosting structures for items to be partially updated
- `nupdatable`: Number of items in the updatable array
## Dependencies
- Functions called/Symbols referenced:
  - : Generates new versions of posting lists without deleted TIDs
  - : Overwrites existing tuples with updated versions
  - : Deletes multiple items from the page
  - , , , , : WAL logging functions
  - , , : Page and relation utility functions
- Called from:
  - : Main entry point for single-page cleanup operations

## Notes and Other Information
- The caller must ensure the buffer is pinned and write-locked before calling this function
- Both deletable and updatable arrays must be sorted in ascending order by offset number
- The function operates within a critical section to ensure atomicity of changes
- Unlike vacuum operations, this function preserves the page's vacuum cycle ID
- WAL logging is conditional based on 
- The function clears the  flag to indicate removal of dead items
- Memory allocated for updated tuples is properly freed to prevent leaks

## Simplified Source

```c
static void _bt_delitems_delete(Relation rel, Buffer buf,
                               TransactionId snapshotConflictHorizon, bool isCatalogRel,
                               OffsetNumber *deletable, int ndeletable,
                               BTVacuumPosting *updatable, int nupdatable) {
    Page page = BufferGetPage(buf);
    BTPageOpaque opaque;
    bool needswal = RelationNeedsWAL(rel);
    char *updatedbuf = NULL;
    Size updatedbuflen = 0;
    OffsetNumber updatedoffsets[MaxIndexTuplesPerPage];

    Assert(ndeletable > 0 || nupdatable > 0);

    // Generate new posting lists with dead TIDs removed
    if (nupdatable > 0)
        updatedbuf = _bt_delitems_update(updatable, nupdatable,
                                       updatedoffsets, &updatedbuflen,
                                       needswal);

    START_CRIT_SECTION();

    // Update posting lists first
    for (int i = 0; i < nupdatable; i++) {
        OffsetNumber updatedoffset = updatedoffsets[i];
        IndexTuple itup = updatable[i]->itup;
        Size itemsz = MAXALIGN(IndexTupleSize(itup));

        if (!PageIndexTupleOverwrite(page, updatedoffset, (Item) itup, itemsz))
            elog(PANIC, "failed to update partially dead item in block %u",
                 BufferGetBlockNumber(buf));
    }

    // Delete entire tuples
    if (ndeletable > 0)
        PageIndexMultiDelete(page, deletable, ndeletable);

    // Clear garbage flag (but NOT vacuum cycle ID like _bt_delitems_vacuum does)
    opaque = BTPageGetOpaque(page);
    opaque->btpo_flags &= ~BTP_HAS_GARBAGE;

    MarkBufferDirty(buf);

    // WAL logging for DELETE operation (includes conflict horizon)
    if (needswal) {
        XLogRecPtr recptr;
        xl_btree_delete xlrec_delete;

        xlrec_delete.snapshotConflictHorizon = snapshotConflictHorizon;
        xlrec_delete.ndeleted = ndeletable;
        xlrec_delete.nupdated = nupdatable;
        xlrec_delete.isCatalogRel = isCatalogRel;

        XLogBeginInsert();
        XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
        XLogRegisterData((char *) &xlrec_delete, SizeOfBtreeDelete);

        if (ndeletable > 0)
            XLogRegisterBufData(0, (char *) deletable,
                              ndeletable * sizeof(OffsetNumber));

        if (nupdatable > 0) {
            XLogRegisterBufData(0, (char *) updatedoffsets,
                              nupdatable * sizeof(OffsetNumber));
            XLogRegisterBufData(0, updatedbuf, updatedbuflen);
        }

        recptr = XLogInsert(RM_BTREE_ID, XLOG_BTREE_DELETE);
        PageSetLSN(page, recptr);
    }

    END_CRIT_SECTION();

    // Cleanup allocated memory
    if (updatedbuf != NULL)
        pfree(updatedbuf);
    for (int i = 0; i < nupdatable; i++)
        pfree(updatable[i]->itup);
}
```