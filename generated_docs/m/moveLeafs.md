# moveLeafs

## Location
[src/backend/access/spgist/spgdoinsert.c:387-567](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgdoinsert.c#L387-L567)

## Overview
This function moves an entire chain of leaf tuples from one page to another when there isn't enough room to add a new leaf tuple to the current page, used as an alternative to splitting when the chain contains little data.

## Definition

```c
static void
moveLeafs(Relation index, SpGistState *state,
		  SPPageDesc *current, SPPageDesc *parent,
		  SpGistLeafTuple newLeafTuple, bool isNulls)
```
## Detailed Description
This function implements a space optimization strategy for SPGiST indexes. When a leaf tuple chain needs more space but contains very little data (making a split inefficient), it moves the entire chain to a new page along with the new tuple that couldn't fit. The function:

1. Analyzes the current chain to determine space requirements
2. Finds or allocates a new leaf page with sufficient space
3. Copies all live tuples from the old chain to the new page (reversing chain order)
4. Adds the new tuple to the chain on the new page
5. Deletes old tuples and leaves redirection pointers (unless during index build)
6. Updates the parent's downlink to point to the new location
7. Handles WAL logging for crash recovery

The function cannot work on root pages and includes special handling for DEAD tuples.

## Parameters / Member Variables
- `index`: The SPGiST index relation being modified
- `*state`: SPGiST state information containing configuration and temporary data
- `*current`: Page descriptor for the current page containing the tuple chain to move
- `*parent`: Page descriptor for the parent page (must be valid, not root)
- `newLeafTuple`: The new leaf tuple that triggered the move operation
- `isNulls`: Boolean indicating if this is a nulls page
## Dependencies
- Functions called/Symbols referenced:
  - [SpGistGetBuffer](../S/SpGistGetBuffer.md) (allocates a new leaf page with required space)
  - [SpGistPageAddNewItem](../S/SpGistPageAddNewItem.md) (adds items to the new page)
  - [spgPageIndexMultiDelete](../s/spgPageIndexMultiDelete.md) (deletes old tuples and sets redirection)
  - [saveNodeLink](../s/saveNodeLink.md) (updates parent's downlink)
  - SGLT_SET_NEXTOFFSET/SGLT_GET_NEXTOFFSET (chain manipulation macros)
  - Various page access functions (PageGetItem, PageGetItemId, etc.)
  - WAL logging functions (XLogBeginInsert, XLogInsert, etc.)
- Called from (representative examples):
  - [spgdoinsert](../s/spgdoinsert.md) (at src/backend/access/spgist/spgdoinsert.c:2133)

## Notes and Other Information
- Cannot operate on root pages (assertion enforced)
- Reverses the order of tuples in the chain during the move (but this doesn't affect correctness)
- DEAD tuples are deleted but not moved to the new page
- Uses SPGIST_REDIRECT pointers for the first deleted tuple (unless during index build)
- Subsequent deleted tuples get SPGIST_PLACEHOLDER markers
- Operates within critical section for atomicity
- Updates free-space cache with SpGistSetLastUsedPage()
- Supports WAL logging when RelationNeedsWAL() returns true
- Uses spgxlogMoveLeafs structure for WAL record information
- Location: src/backend/access/spgist/spgdoinsert.c:387-567

## Simplified Source

```c
static void moveLeafs(Relation index, SpGistState *state,
                     SPPageDesc *current, SPPageDesc *parent,
                     SpGistLeafTuple newLeafTuple, bool isNulls)
{
    int i, nDelete, nInsert, size;
    Buffer nbuf;
    Page npage;
    OffsetNumber r = InvalidOffsetNumber;
    bool replaceDead = false;
    OffsetNumber *toDelete, *toInsert;
    BlockNumber nblkno;
    spgxlogMoveLeafs xlrec;
    char *leafdata, *leafptr;

    // Allocate arrays for tracking tuple movements
    i = PageGetMaxOffsetNumber(current->page);
    toDelete = (OffsetNumber *) palloc(sizeof(OffsetNumber) * i);
    toInsert = (OffsetNumber *) palloc(sizeof(OffsetNumber) * (i + 1));

    // Calculate space needed including new tuple
    size = newLeafTuple->size + sizeof(ItemIdData);

    // Walk chain to find tuples to move and calculate total size
    nDelete = 0;
    i = current->offnum;
    while (i != InvalidOffsetNumber)
    {
        SpGistLeafTuple it = (SpGistLeafTuple) PageGetItem(current->page,
                                                          PageGetItemId(current->page, i));

        if (it->tupstate == SPGIST_LIVE)
        {
            toDelete[nDelete] = i;
            size += it->size + sizeof(ItemIdData);
            nDelete++;
        }
        else if (it->tupstate == SPGIST_DEAD)
        {
            // Mark dead tuple for deletion but don't include in size
            toDelete[nDelete] = i;
            nDelete++;
            replaceDead = true;
        }

        i = SGLT_GET_NEXTOFFSET(it);
    }

    // Get new page with enough space
    nbuf = SpGistGetBuffer(index, GBUF_LEAF | (isNulls ? GBUF_NULLS : 0),
                          size, &xlrec.newPage);
    npage = BufferGetPage(nbuf);
    nblkno = BufferGetBlockNumber(nbuf);

    leafdata = leafptr = palloc(size);

    START_CRIT_SECTION();

    // Copy live tuples to new page (chain order gets reversed)
    nInsert = 0;
    if (!replaceDead)
    {
        for (i = 0; i < nDelete; i++)
        {
            SpGistLeafTuple it = (SpGistLeafTuple) PageGetItem(current->page,
                                                              PageGetItemId(current->page, toDelete[i]));
            if (it->tupstate == SPGIST_LIVE)
            {
                // Update chain linkage and add to new page
                SGLT_SET_NEXTOFFSET(it, r);
                r = SpGistPageAddNewItem(state, npage, (Item) it, it->size, NULL, false);
                toInsert[nInsert] = r;
                nInsert++;

                // Save tuple data for WAL
                memcpy(leafptr, it, it->size);
                leafptr += it->size;
            }
        }
    }

    // Add the new tuple to the chain
    SGLT_SET_NEXTOFFSET(newLeafTuple, r);
    r = SpGistPageAddNewItem(state, npage, (Item) newLeafTuple, newLeafTuple->size, NULL, false);
    toInsert[nInsert] = r;
    nInsert++;
    memcpy(leafptr, newLeafTuple, newLeafTuple->size);

    // Delete old tuples and set redirection pointers
    spgPageIndexMultiDelete(state, current->page, toDelete, nDelete,
                           state->isBuild ? SPGIST_PLACEHOLDER : SPGIST_REDIRECT,
                           SPGIST_PLACEHOLDER, nblkno, r);

    // Update parent to point to new location
    saveNodeLink(index, parent, nblkno, r);

    // Mark buffers dirty
    MarkBufferDirty(current->buffer);
    MarkBufferDirty(nbuf);

    // WAL logging if needed
    if (RelationNeedsWAL(index) && !state->isBuild)
    {
        XLogRecPtr recptr;
        STORE_STATE(state, xlrec.stateSrc);
        xlrec.nMoves = nDelete;
        xlrec.replaceDead = replaceDead;
        xlrec.storesNulls = isNulls;
        xlrec.offnumParent = parent->offnum;
        xlrec.nodeI = parent->node;

        XLogBeginInsert();
        XLogRegisterData((char *) &xlrec, SizeOfSpgxlogMoveLeafs);
        XLogRegisterData((char *) toDelete, sizeof(OffsetNumber) * nDelete);
        XLogRegisterData((char *) toInsert, sizeof(OffsetNumber) * nInsert);
        XLogRegisterData((char *) leafdata, leafptr - leafdata);

        XLogRegisterBuffer(0, current->buffer, REGBUF_STANDARD);
        XLogRegisterBuffer(1, nbuf, REGBUF_STANDARD | (xlrec.newPage ? REGBUF_WILL_INIT : 0));
        XLogRegisterBuffer(2, parent->buffer, REGBUF_STANDARD);

        recptr = XLogInsert(RM_SPGIST_ID, XLOG_SPGIST_MOVE_LEAFS);
        PageSetLSN(current->page, recptr);
        PageSetLSN(npage, recptr);
        PageSetLSN(parent->page, recptr);
    }

    END_CRIT_SECTION();

    // Release new buffer
    SpGistSetLastUsedPage(index, nbuf);
    UnlockReleaseBuffer(nbuf);
}
```