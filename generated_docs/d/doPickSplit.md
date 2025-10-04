# doPickSplit

## Location
[src/backend/access/spgist/spgdoinsert.c:677-1458](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgdoinsert.c#L677-L1458)

## Overview
Splits a leaf tuple chain when there's insufficient space to add a new leaf tuple, redistributing tuples across current and new pages according to picksplit algorithm rules.

## Definition

```c
static bool
doPickSplit(Relation index, SpGistState *state,
			SPPageDesc *current, SPPageDesc *parent,
			SpGistLeafTuple newLeafTuple,
			int level, bool isNulls, bool isNew)
```
## Detailed Description
The  function handles the complex process of splitting leaf tuple chains in SP-GiST when a page lacks sufficient space for a new tuple. It creates one or more new chains distributed across the current page and an additional leaf page, while creating a new inner tuple to organize the split result. The function uses the opclass-defined picksplit method to determine how to redistribute tuples, potentially stripping prefixes to make tuples smaller. The split ensures at least two chains are created, guaranteeing forward progress even with unbalanced splits.

## Parameters / Member Variables
- `index`: The SP-GiST index relation being modified
- `*state`: SP-GiST state information containing opclass details and configuration
- `*current`: Page descriptor for the current page containing the leaf tuple chain
- `*parent`: Page descriptor for the parent page (containing the downlink to current)
- `newLeafTuple`: The new leaf tuple that triggered the need for splitting
- `level`: Current tree level for prefix stripping decisions
- `isNulls`: Whether this operation is on the nulls tree
- `isNew`: Whether the current page is newly created
## Dependencies
- Functions called/Symbols referenced:
  - [checkAllTheSame](../c/checkAllTheSame.md)
  - [spgFormLeafTuple](../s/spgFormLeafTuple.md)  
  - [spgFormInnerTuple](../s/spgFormInnerTuple.md)
  - [spgFormNodeTuple](../s/spgFormNodeTuple.md)
  - [SpGistGetBuffer](../S/SpGistGetBuffer.md)
  - [SpGistPageAddNewItem](../S/SpGistPageAddNewItem.md)
  - [saveNodeLink](../s/saveNodeLink.md)
  - [setRedirectionTuple](../s/setRedirectionTuple.md)
- Called from (representative examples):
  - [spgdoinsert](../s/spgdoinsert.md)

## Notes and Other Information
Returns true if the new leaf tuple was successfully inserted during the split operation, false if the caller needs to retry (typically due to space constraints or unbalanced splits). The function handles WAL logging for crash recovery and manages buffer locking to prevent deadlocks. Special handling is required for root page splits, which transform a leaf page into an inner page. The algorithm may require multiple iterations if the picksplit result is highly unbalanced or if prefix stripping is insufficient to make tuples fit.

## Simplified Source

```c
static bool doPickSplit(Relation index, SpGistState *state,
                       SPPageDesc *current, SPPageDesc *parent,
                       SpGistLeafTuple newLeafTuple,
                       int level, bool isNulls, bool isNew)
{
    bool insertedNew = false;
    spgPickSplitIn in;
    spgPickSplitOut out;
    SpGistInnerTuple innerTuple;
    Buffer newLeafBuffer;
    OffsetNumber *toDelete, *toInsert;
    SpGistLeafTuple *oldLeafs, *newLeafs;
    uint8 *leafPageSelect;
    spgxlogPickSplit xlrec;

    // Setup input for picksplit function
    in.level = level;

    // Collect existing leaf tuples from chain (or all tuples if root split)
    if (SpGistBlockIsRoot(current->blkno))
    {
        // Root split: collect all tuples on page
        for (i = FirstOffsetNumber; i <= max; i++)
        {
            // Collect live tuples for splitting
        }
    }
    else
    {
        // Normal split: follow the tuple chain
        i = current->offnum;
        while (i != InvalidOffsetNumber)
        {
            // Collect live tuples from chain
            i = SGLT_GET_NEXTOFFSET(it);
        }
    }

    // Add new tuple to split input
    in.datums[in.nTuples] = isNulls ? (Datum) 0 : SGLTDATUM(newLeafTuple, state);
    oldLeafs[in.nTuples] = newLeafTuple;
    in.nTuples++;

    // Call opclass picksplit function to determine redistribution
    if (!isNulls)
    {
        procinfo = index_getprocinfo(index, 1, SPGIST_PICKSPLIT_PROC);
        FunctionCall2Coll(procinfo, index->rd_indcollation[0],
                         PointerGetDatum(&in), PointerGetDatum(&out));

        // Form new leaf tuples based on split result
        for (i = 0; i < in.nTuples; i++)
        {
            leafDatums[spgKeyColumn] = out.leafTupleDatums[i];
            newLeafs[i] = spgFormLeafTuple(state, &oldLeafs[i]->heapPtr,
                                         leafDatums, leafIsnulls);
        }
    }

    // Check if split actually separated values
    allTheSame = checkAllTheSame(&in, &out, totalLeafSizes > SPGIST_PAGE_CAPACITY, &includeNew);

    // Create inner tuple to organize the split result
    for (i = 0; i < out.nNodes; i++)
    {
        nodes[i] = spgFormNodeTuple(state, out.nodeLabels[i], labelisnull);
    }
    innerTuple = spgFormInnerTuple(state, out.hasPrefix, out.prefixDatum,
                                  out.nNodes, nodes);

    // Determine page placement for inner tuple
    if (parent->buffer != InvalidBuffer && !SpGistBlockIsRoot(parent->blkno) &&
        SpGistPageGetFreeSpace(parent->page, 1) >= innerTuple->size + sizeof(ItemIdData))
    {
        newInnerBuffer = parent->buffer;  // Fits on parent page
    }
    else if (parent->buffer != InvalidBuffer)
    {
        newInnerBuffer = SpGistGetBuffer(index, GBUF_INNER_PARITY(parent->blkno + 1) |
                                       (isNulls ? GBUF_NULLS : 0),
                                       innerTuple->size + sizeof(ItemIdData), &xlrec.initInner);
    }
    else
    {
        newInnerBuffer = InvalidBuffer;  // Root split
    }

    // Determine if we need a new leaf page
    if (totalLeafSizes <= currentFreeSpace)
    {
        newLeafBuffer = InvalidBuffer;  // All fits on current page
        insertedNew = includeNew;
    }
    else
    {
        newLeafBuffer = SpGistGetBuffer(index, GBUF_LEAF | (isNulls ? GBUF_NULLS : 0),
                                      Min(totalLeafSizes, SPGIST_PAGE_CAPACITY), &xlrec.initDest);

        // Assign node groups to pages
        // [Space allocation logic for distributing tuples across pages]
        insertedNew = includeNew;
    }

    START_CRIT_SECTION();

    // Delete old tuples (except for root splits)
    if (!SpGistBlockIsRoot(current->blkno))
    {
        if (state->isBuild && nToDelete + SpGistPageGetOpaque(current->page)->nPlaceholder ==
            PageGetMaxOffsetNumber(current->page))
        {
            SpGistInitBuffer(current->buffer, SPGIST_LEAF | (isNulls ? SPGIST_NULLS : 0));
        }
        else
        {
            spgPageIndexMultiDelete(state, current->page, toDelete, nToDelete,
                                   state->isBuild ? SPGIST_PLACEHOLDER : SPGIST_REDIRECT,
                                   SPGIST_PLACEHOLDER, SPGIST_METAPAGE_BLKNO, FirstOffsetNumber);
        }
    }

    // Place leaf tuples on appropriate pages and link to inner tuple nodes
    for (i = 0; i < nToInsert; i++)
    {
        Buffer leafBuffer = leafPageSelect[i] ? newLeafBuffer : current->buffer;
        BlockNumber leafBlock = BufferGetBlockNumber(leafBuffer);

        // Link tuple into correct chain for its node
        n = out.mapTuplesToNodes[i];
        if (ItemPointerIsValid(&nodes[n]->t_tid))
        {
            SGLT_SET_NEXTOFFSET(newLeafs[i], ItemPointerGetOffsetNumber(&nodes[n]->t_tid));
        }

        // Insert tuple and update node downlink
        OffsetNumber newoffset = SpGistPageAddNewItem(state, BufferGetPage(leafBuffer),
                                                     (Item) newLeafs[i], newLeafs[i]->size, NULL, false);
        ItemPointerSet(&nodes[n]->t_tid, leafBlock, newoffset);
    }

    // Store the new inner tuple
    if (newInnerBuffer == parent->buffer && newInnerBuffer != InvalidBuffer)
    {
        // Inner tuple goes on parent page
        current->offnum = SpGistPageAddNewItem(state, parent->page,
                                             (Item) innerTuple, innerTuple->size, NULL, false);
        saveNodeLink(index, parent, current->blkno, current->offnum);
    }
    else if (parent->buffer != InvalidBuffer)
    {
        // Inner tuple goes on new page
        current->buffer = newInnerBuffer;
        current->offnum = SpGistPageAddNewItem(state, BufferGetPage(newInnerBuffer),
                                             (Item) innerTuple, innerTuple->size, NULL, false);
        saveNodeLink(index, parent, current->blkno, current->offnum);
    }
    else
    {
        // Root split: initialize root page as inner page
        SpGistInitBuffer(current->buffer, (isNulls ? SPGIST_NULLS : 0));
        current->offnum = PageAddItem(current->page, (Item) innerTuple, innerTuple->size,
                                    InvalidOffsetNumber, false, false);
    }

    // Mark all modified buffers dirty
    MarkBufferDirty(current->buffer);
    if (newLeafBuffer != InvalidBuffer)
        MarkBufferDirty(newLeafBuffer);

    // WAL logging
    if (RelationNeedsWAL(index) && !state->isBuild)
    {
        XLogRecPtr recptr;
        XLogBeginInsert();
        XLogRegisterData((char *) &xlrec, SizeOfSpgxlogPickSplit);
        // [Register all data and buffers for WAL]
        recptr = XLogInsert(RM_SPGIST_ID, XLOG_SPGIST_PICKSPLIT);
        // [Set LSN on all pages]
    }

    END_CRIT_SECTION();

    // Cleanup and release buffers
    if (newLeafBuffer != InvalidBuffer)
    {
        SpGistSetLastUsedPage(index, newLeafBuffer);
        UnlockReleaseBuffer(newLeafBuffer);
    }

    return insertedNew;
}
```