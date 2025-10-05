# spgAddNodeAction

## Location
[src/backend/access/spgist/spgdoinsert.c:1513-1714](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgdoinsert.c#L1513-L1714)

## Overview
Adds a new node to an existing inner tuple, either by in-place replacement or by moving the enlarged tuple to a new page and updating parent references.

## Definition

```c
struct new inner tuple with additional node */
	newInnerTuple = addNode(state, innerTuple, nodeLabel, nodeN);
```
## Detailed Description
The  function handles the "addNode" operation requested by the opclass choose function. It creates a new version of the inner tuple with an additional node inserted at the specified position. If the enlarged tuple fits on the current page, it performs in-place replacement. Otherwise, it allocates a new page, moves the tuple there, updates the parent's downlink, and replaces the original tuple with either a redirection tuple (during normal operation) or a placeholder (during index build) to maintain tuple offset stability for existing downlinks.

## Parameters / Member Variables
- : The SP-GiST index relation being modified
- : SP-GiST state information containing opclass configuration
- : The existing inner tuple to be enlarged with a new node
- : Page descriptor for the page containing the inner tuple
- : Page descriptor for the parent page (needed if tuple must be moved)
- : Position where the new node should be inserted
- : Label value for the new node being added

## Dependencies
- Functions called/Symbols referenced:
  - [addNode](../a/addNode.md)
  - [PageGetExactFreeSpace](../P/PageGetExactFreeSpace.md)
  - [PageIndexTupleDelete](../P/PageIndexTupleDelete.md)
  - PageAddItem
  - [SpGistGetBuffer](../S/SpGistGetBuffer.md)
  - [SpGistPageAddNewItem](../S/SpGistPageAddNewItem.md)
  - [saveNodeLink](saveNodeLink.md)
  - [spgFormDeadTuple](spgFormDeadTuple.md)
- Called from (representative examples):
  - [spgdoinsert](spgdoinsert.md)

## Notes and Other Information
The function includes comprehensive WAL logging for crash recovery when not in build mode. It cannot be applied to nulls pages and will error if attempted on the root page when enlargement would exceed page capacity. The function carefully manages buffer relationships, ensuring that parent buffer updates are properly coordinated when the tuple is moved to a different page. During index build, placeholder tuples are used instead of redirection tuples for better performance since concurrent scans are not a concern.

## Simplified Source

```c
static void
spgAddNodeAction(Relation index, SpGistState *state,
                 SpGistInnerTuple innerTuple,
                 SPPageDesc *current, SPPageDesc *parent,
                 int nodeN, Datum nodeLabel)
{
    SpGistInnerTuple newInnerTuple;
    spgxlogAddNode xlrec;

    // Create new inner tuple with additional node
    newInnerTuple = addNode(state, innerTuple, nodeLabel, nodeN);

    // Prepare WAL record structure
    STORE_STATE(state, xlrec.stateSrc);
    xlrec.offnum = current->offnum;

    // Initialize parent and move fields
    xlrec.parentBlk = -1;
    xlrec.offnumParent = InvalidOffsetNumber;
    xlrec.offnumNew = InvalidOffsetNumber;
    xlrec.newPage = false;

    if (PageGetExactFreeSpace(current->page) >=
        newInnerTuple->size - innerTuple->size)
    {
        // In-place replacement: sufficient space on current page
        START_CRIT_SECTION();

        PageIndexTupleDelete(current->page, current->offnum);
        PageAddItem(current->page, (Item) newInnerTuple, newInnerTuple->size,
                   current->offnum, false, false);

        MarkBufferDirty(current->buffer);

        // WAL logging for in-place update
        if (RelationNeedsWAL(index) && !state->isBuild)
        {
            XLogBeginInsert();
            XLogRegisterData((char *) &xlrec, sizeof(xlrec));
            XLogRegisterData((char *) newInnerTuple, newInnerTuple->size);
            XLogRegisterBuffer(0, current->buffer, REGBUF_STANDARD);

            XLogRecPtr recptr = XLogInsert(RM_SPGIST_ID, XLOG_SPGIST_ADD_NODE);
            PageSetLSN(current->page, recptr);
        }

        END_CRIT_SECTION();
    }
    else
    {
        // Move to new page: insufficient space for enlarged tuple
        SpGistDeadTuple deadTuple;
        SPPageDesc saveCurrent = *current;

        // Set up parent update information
        xlrec.offnumParent = parent->offnum;
        xlrec.nodeI = parent->node;

        // Allocate new buffer with same parity
        current->buffer = SpGistGetBuffer(index,
                                        GBUF_INNER_PARITY(current->blkno),
                                        newInnerTuple->size + sizeof(ItemIdData),
                                        &xlrec.newPage);
        current->blkno = BufferGetBlockNumber(current->buffer);
        current->page = BufferGetPage(current->buffer);

        // Determine buffer relationships for WAL
        if (parent->buffer == saveCurrent.buffer)
            xlrec.parentBlk = 0;
        else if (parent->buffer == current->buffer)
            xlrec.parentBlk = 1;
        else
            xlrec.parentBlk = 2;

        START_CRIT_SECTION();

        // Insert new tuple on new page
        xlrec.offnumNew = current->offnum =
            SpGistPageAddNewItem(state, current->page,
                               (Item) newInnerTuple, newInnerTuple->size,
                               NULL, false);
        MarkBufferDirty(current->buffer);

        // Update parent's downlink
        saveNodeLink(index, parent, current->blkno, current->offnum);

        // Replace old tuple with placeholder/redirection
        if (state->isBuild)
            deadTuple = spgFormDeadTuple(state, SPGIST_PLACEHOLDER,
                                       InvalidBlockNumber, InvalidOffsetNumber);
        else
            deadTuple = spgFormDeadTuple(state, SPGIST_REDIRECT,
                                       current->blkno, current->offnum);

        PageIndexTupleDelete(saveCurrent.page, saveCurrent.offnum);
        PageAddItem(saveCurrent.page, (Item) deadTuple, deadTuple->size,
                   saveCurrent.offnum, false, false);

        // Update page statistics
        if (state->isBuild)
            SpGistPageGetOpaque(saveCurrent.page)->nPlaceholder++;
        else
            SpGistPageGetOpaque(saveCurrent.page)->nRedirection++;

        MarkBufferDirty(saveCurrent.buffer);

        // WAL logging for move operation
        if (RelationNeedsWAL(index) && !state->isBuild)
        {
            XLogBeginInsert();
            XLogRegisterBuffer(0, saveCurrent.buffer, REGBUF_STANDARD);

            int flags = REGBUF_STANDARD;
            if (xlrec.newPage) flags |= REGBUF_WILL_INIT;
            XLogRegisterBuffer(1, current->buffer, flags);

            if (xlrec.parentBlk == 2)
                XLogRegisterBuffer(2, parent->buffer, REGBUF_STANDARD);

            XLogRegisterData((char *) &xlrec, sizeof(xlrec));
            XLogRegisterData((char *) newInnerTuple, newInnerTuple->size);

            XLogRecPtr recptr = XLogInsert(RM_SPGIST_ID, XLOG_SPGIST_ADD_NODE);
            PageSetLSN(current->page, recptr);
            PageSetLSN(parent->page, recptr);
            PageSetLSN(saveCurrent.page, recptr);
        }

        END_CRIT_SECTION();

        // Clean up old buffer if different from current/parent
        if (saveCurrent.buffer != current->buffer &&
            saveCurrent.buffer != parent->buffer)
        {
            SpGistSetLastUsedPage(index, saveCurrent.buffer);
            UnlockReleaseBuffer(saveCurrent.buffer);
        }
    }
}
```