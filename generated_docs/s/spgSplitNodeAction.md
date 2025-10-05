# spgSplitNodeAction

## Location
[src/backend/access/spgist/spgdoinsert.c:1715-1913](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgdoinsert.c#L1715-L1913)

## Overview
Splits an inner tuple into prefix and postfix tuples according to opclass specifications, replacing the original tuple with the prefix and linking it to the newly created postfix tuple.

## Definition

```c
struct new prefix tuple with requested number of nodes.  We'll fill
	 * in the childNodeN'th node's downlink below.
	 */
	nodes = (SpGistNodeTuple *) palloc(sizeof(SpGistNodeTuple) *
									   out->result.splitTuple.prefixNNodes);
```
## Detailed Description
The  function implements inner tuple splitting as requested by the opclass choose function. It constructs a new prefix tuple with the specified number of nodes and prefix information, and a postfix tuple containing all original nodes but with updated prefix data. The prefix tuple replaces the original tuple on the current page, while the postfix tuple may be placed on the same page (if space permits) or moved to a new page following triple parity rules. The function ensures proper downlink establishment from the prefix tuple's specified child node to the postfix tuple location.

## Parameters / Member Variables
- : The SP-GiST index relation being modified
- : SP-GiST state information for tuple formation
- : The existing inner tuple to be split
- : Page descriptor for the page containing the inner tuple
- : Choose function output containing split specifications and parameters

## Dependencies
- Functions called/Symbols referenced:
  - [spgFormNodeTuple](spgFormNodeTuple.md)
  - [spgFormInnerTuple](spgFormInnerTuple.md)  
  - [SpGistGetBuffer](../S/SpGistGetBuffer.md)
  - [PageIndexTupleDelete](../P/PageIndexTupleDelete.md)
  - PageAddItem
  - [SpGistPageAddNewItem](../S/SpGistPageAddNewItem.md)
  - [spgUpdateNodeLink](spgUpdateNodeLink.md)
  - SGITITERATE
- Called from (representative examples):
  - [spgdoinsert](spgdoinsert.md)

## Notes and Other Information
The function validates that the opclass provided reasonable split parameters (valid node counts and child node numbers). It cannot be applied to nulls pages and includes special handling for root page splits where both tuples cannot fit on the same page. The postfix tuple inherits the allTheSame property from the original tuple. WAL logging ensures crash recovery capability, and the function manages page allocation using triple parity rules to maintain proper tree structure. The prefix tuple must not exceed the size of the original tuple to ensure it fits in the replacement location.

## Simplified Source

```c
static void
spgSplitNodeAction(Relation index, SpGistState *state,
                   SpGistInnerTuple innerTuple,
                   SPPageDesc *current, spgChooseOut *out)
{
    SpGistInnerTuple prefixTuple, postfixTuple;
    SpGistNodeTuple node, *nodes;
    BlockNumber postfixBlkno;
    OffsetNumber postfixOffset;
    spgxlogSplitTuple xlrec;
    Buffer newBuffer = InvalidBuffer;

    // Validate opclass split parameters
    if (out->result.splitTuple.prefixNNodes <= 0 ||
        out->result.splitTuple.prefixNNodes > SGITMAXNNODES)
        elog(ERROR, "invalid number of prefix nodes: %d",
             out->result.splitTuple.prefixNNodes);

    if (out->result.splitTuple.childNodeN < 0 ||
        out->result.splitTuple.childNodeN >= out->result.splitTuple.prefixNNodes)
        elog(ERROR, "invalid child node number: %d",
             out->result.splitTuple.childNodeN);

    // Construct prefix tuple with requested number of nodes
    nodes = (SpGistNodeTuple *) palloc(sizeof(SpGistNodeTuple) *
                                      out->result.splitTuple.prefixNNodes);

    for (int i = 0; i < out->result.splitTuple.prefixNNodes; i++)
    {
        Datum label = (Datum) 0;
        bool labelisnull = (out->result.splitTuple.prefixNodeLabels == NULL);

        if (!labelisnull)
            label = out->result.splitTuple.prefixNodeLabels[i];

        nodes[i] = spgFormNodeTuple(state, label, labelisnull);
    }

    prefixTuple = spgFormInnerTuple(state,
                                   out->result.splitTuple.prefixHasPrefix,
                                   out->result.splitTuple.prefixPrefixDatum,
                                   out->result.splitTuple.prefixNNodes,
                                   nodes);

    // Prefix must fit in space that original tuple occupied
    if (prefixTuple->size > innerTuple->size)
        elog(ERROR, "SPGiST inner-tuple split must not produce longer prefix");

    // Construct postfix tuple with all original nodes but new prefix
    nodes = palloc(sizeof(SpGistNodeTuple) * innerTuple->nNodes);
    SGITITERATE(innerTuple, i, node)
    {
        nodes[i] = node;
    }

    postfixTuple = spgFormInnerTuple(state,
                                    out->result.splitTuple.postfixHasPrefix,
                                    out->result.splitTuple.postfixPrefixDatum,
                                    innerTuple->nNodes, nodes);

    // Preserve allTheSame property
    postfixTuple->allTheSame = innerTuple->allTheSame;

    xlrec.newPage = false;

    // Check if both tuples fit on current page
    if (SpGistBlockIsRoot(current->blkno) ||
        SpGistPageGetFreeSpace(current->page, 1) + innerTuple->size <
        prefixTuple->size + postfixTuple->size + sizeof(ItemIdData))
    {
        // Need new page for postfix tuple (triple parity rule)
        newBuffer = SpGistGetBuffer(index,
                                   GBUF_INNER_PARITY(current->blkno + 1),
                                   postfixTuple->size + sizeof(ItemIdData),
                                   &xlrec.newPage);
    }

    START_CRIT_SECTION();

    // Replace original tuple with prefix tuple
    PageIndexTupleDelete(current->page, current->offnum);
    xlrec.offnumPrefix = PageAddItem(current->page,
                                    (Item) prefixTuple, prefixTuple->size,
                                    current->offnum, false, false);

    // Insert postfix tuple on appropriate page
    if (newBuffer == InvalidBuffer)
    {
        // Same page
        postfixBlkno = current->blkno;
        xlrec.offnumPostfix = postfixOffset =
            SpGistPageAddNewItem(state, current->page,
                               (Item) postfixTuple, postfixTuple->size,
                               NULL, false);
        xlrec.postfixBlkSame = true;
    }
    else
    {
        // New page
        postfixBlkno = BufferGetBlockNumber(newBuffer);
        xlrec.offnumPostfix = postfixOffset =
            SpGistPageAddNewItem(state, BufferGetPage(newBuffer),
                               (Item) postfixTuple, postfixTuple->size,
                               NULL, false);
        MarkBufferDirty(newBuffer);
        xlrec.postfixBlkSame = false;
    }

    // Update downlink in prefix tuple to point to postfix tuple
    spgUpdateNodeLink(prefixTuple, out->result.splitTuple.childNodeN,
                     postfixBlkno, postfixOffset);

    // Also update the on-page copy
    prefixTuple = (SpGistInnerTuple) PageGetItem(current->page,
                                                PageGetItemId(current->page, current->offnum));
    spgUpdateNodeLink(prefixTuple, out->result.splitTuple.childNodeN,
                     postfixBlkno, postfixOffset);

    MarkBufferDirty(current->buffer);

    // WAL logging for split operation
    if (RelationNeedsWAL(index) && !state->isBuild)
    {
        XLogBeginInsert();
        XLogRegisterData((char *) &xlrec, sizeof(xlrec));
        XLogRegisterData((char *) prefixTuple, prefixTuple->size);
        XLogRegisterData((char *) postfixTuple, postfixTuple->size);

        XLogRegisterBuffer(0, current->buffer, REGBUF_STANDARD);
        if (newBuffer != InvalidBuffer)
        {
            int flags = REGBUF_STANDARD;
            if (xlrec.newPage) flags |= REGBUF_WILL_INIT;
            XLogRegisterBuffer(1, newBuffer, flags);
        }

        XLogRecPtr recptr = XLogInsert(RM_SPGIST_ID, XLOG_SPGIST_SPLIT_TUPLE);
        PageSetLSN(current->page, recptr);

        if (newBuffer != InvalidBuffer)
            PageSetLSN(BufferGetPage(newBuffer), recptr);
    }

    END_CRIT_SECTION();

    // Clean up new buffer
    if (newBuffer != InvalidBuffer)
    {
        SpGistSetLastUsedPage(index, newBuffer);
        UnlockReleaseBuffer(newBuffer);
    }
}
```