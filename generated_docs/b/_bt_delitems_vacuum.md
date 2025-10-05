# _bt_delitems_vacuum

## Location
[src/backend/access/nbtree/nbtpage.c:1154-1283](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtpage.c#L1154-L1283)

## Overview
_bt_delitems_vacuum removes dead tuples and updates posting lists on B-tree leaf pages during VACUUM operations, handling both simple deletions and partial posting list updates with proper WAL logging.

## Definition

```c
void
_bt_delitems_vacuum(Relation rel, Buffer buf,
					OffsetNumber *deletable, int ndeletable,
					BTVacuumPosting *updatable, int nupdatable)
```
## Detailed Description
_bt_delitems_vacuum is the primary function for cleaning B-tree leaf pages during VACUUM operations. It handles two types of tuple maintenance: complete removal of dead tuples and selective removal of dead heap TIDs from posting list tuples (where multiple heap TIDs point to the same index key).

The function operates in two phases. First, it processes posting list updates by generating new versions of index tuples with dead heap TIDs removed, then overwrites the original tuples on the page. Second, it performs simple deletions of entirely dead tuples using PageIndexMultiDelete.

The function includes comprehensive WAL logging specific to VACUUM operations, which differs from regular B-tree deletion WAL records. VACUUM WAL records don't need to generate snapshot conflict horizons directly since the initial VACUUM table scan handles this indirectly for all indexes.

Additionally, the function performs page maintenance by clearing the VACUUM cycle ID and removing the BTP_HAS_GARBAGE flag, indicating the page has been cleaned and processed by the current VACUUM cycle.

## Parameters / Member Variables
- `rel`: The B-tree index relation being vacuumed
- `buf`: Buffer containing the leaf page to be cleaned (must have cleanup lock)
- `*deletable`: Array of offset numbers for tuples to be completely deleted
- `ndeletable`: Number of entries in the deletable array
- `*updatable`: Array of BTVacuumPosting structures for posting lists to be updated
- `nupdatable`: Number of entries in the updatable array
## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md)
  - RelationNeedsWAL
  - [_bt_delitems_update](_bt_delitems_update.md)
  - START_CRIT_SECTION
  - [PageIndexTupleOverwrite](../P/PageIndexTupleOverwrite.md)
  - IndexTupleSize
  - [PageIndexMultiDelete](../P/PageIndexMultiDelete.md)
  - BTPageGetOpaque
  - [MarkBufferDirty](../M/MarkBufferDirty.md)
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogRegisterBuffer](../X/XLogRegisterBuffer.md)
  - [XLogRegisterData](../X/XLogRegisterData.md)
  - [XLogRegisterBufData](../X/XLogRegisterBufData.md)
  - [XLogInsert](../X/XLogInsert.md)
  - [PageSetLSN](../P/PageSetLSN.md)
  - END_CRIT_SECTION
  - [pfree](../p/pfree.md)

- Called from (representative examples):
  - [btvacuumpage](btvacuumpage.md)

## Notes and Other Information
- Requires caller to hold a full cleanup lock on the buffer before calling
- Both deletable and updatable arrays must be sorted in ascending order
- Processes posting list updates before simple deletions to avoid offset number complications
- Clears VACUUM cycle ID and BTP_HAS_GARBAGE flag as part of page maintenance
- Uses critical sections to ensure atomicity of page modifications and WAL logging
- Handles memory management for temporary buffers and updated posting list tuples
- WAL logging is VACUUM-specific and differs from regular B-tree deletion logging
- Essential for maintaining B-tree index health and preventing index bloat during VACUUM operations
- Function must have at least one operation to perform (ndeletable > 0 || nupdatable > 0)

## Simplified Source

```c
void _bt_delitems_vacuum(Relation rel, Buffer buf,
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

    // Update posting lists first (before simple deletes to avoid offset complications)
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

    // Clear vacuum cycle ID and garbage flag
    opaque = BTPageGetOpaque(page);
    opaque->btpo_cycleid = 0;
    opaque->btpo_flags &= ~BTP_HAS_GARBAGE;

    MarkBufferDirty(buf);

    // WAL logging for VACUUM operation
    if (needswal) {
        XLogRecPtr recptr;
        xl_btree_vacuum xlrec_vacuum;

        xlrec_vacuum.ndeleted = ndeletable;
        xlrec_vacuum.nupdated = nupdatable;

        XLogBeginInsert();
        XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
        XLogRegisterData((char *) &xlrec_vacuum, SizeOfBtreeVacuum);

        if (ndeletable > 0)
            XLogRegisterBufData(0, (char *) deletable,
                              ndeletable * sizeof(OffsetNumber));

        if (nupdatable > 0) {
            XLogRegisterBufData(0, (char *) updatedoffsets,
                              nupdatable * sizeof(OffsetNumber));
            XLogRegisterBufData(0, updatedbuf, updatedbuflen);
        }

        recptr = XLogInsert(RM_BTREE_ID, XLOG_BTREE_VACUUM);
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