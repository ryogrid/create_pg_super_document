# vacuumLeafPage

## Location
[src/backend/access/spgist/spgvacuum.c:125-407](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgvacuum.c#L125-L407)

## Overview
Vacuums a regular (non-root) leaf page in an SP-GiST index, deleting tuples targeted for deletion while preserving chain structure and handling concurrent redirections.

## Definition
```c
static void vacuumLeafPage(spgBulkDeleteState *bds, Relation index, Buffer buffer, bool forPending)
```

## Detailed Description
This complex function performs vacuum operations on SP-GiST leaf pages with sophisticated chain management. It identifies tuples to delete based on the vacuum callback, but carefully preserves tuple chains by not moving tuples referenced by outside links (assumed to be chain heads).

The function handles three types of tuple states:
- **LIVE tuples**: Checked against vacuum callback, with chain links tracked
- **REDIRECT tuples**: Added to pending list if created by concurrent transactions
- **Other states**: Validated for consistency

The vacuum process operates in several phases:
1. **Scan phase**: Identifies deletable tuples and builds predecessor map
2. **Planning phase**: Determines exact operations needed (dead, placeholder, move, chain updates)
3. **Execution phase**: Performs operations within critical section with WAL logging

Chain management is sophisticated - the function processes entire chains to maintain consistency, using placeholder tuples for mid-chain deletions and moving tuples when necessary to preserve chain heads.

## Parameters / Member Variables
- `bds`: Pointer to spgBulkDeleteState containing vacuum state and callback function
- `index`: The SP-GiST index relation being vacuumed
- `buffer`: Buffer containing the leaf page to vacuum
- `forPending`: Boolean indicating if this call is from pending list processing (affects statistics counting)

## Dependencies
- Functions called/Symbols referenced:
  - [spgAddPendingTID](../s/spgAddPendingTID.md): Adds redirect targets to pending list
  - [spgPageIndexMultiDelete](../s/spgPageIndexMultiDelete.md): Performs bulk tuple state changes
  - [BufferGetPage](../B/BufferGetPage.md), PageGetItem, PageGetItemId: Page access functions
  - [TransactionIdFollowsOrEquals](../T/TransactionIdFollowsOrEquals.md): Transaction visibility check
  - XLog functions: WAL logging (XLogBeginInsert, XLogInsert, etc.)
  - Various SP-GiST tuple access macros (SGLT_GET_NEXTOFFSET, etc.)
- Called from (representative examples):
  - [spgvacuumpage](../s/spgvacuumpage.md): Main vacuum entry point for sequential page processing
  - [spgprocesspending](../s/spgprocesspending.md): Called when processing pending redirect targets

## Notes and Other Information
- This is a static function within the spgvacuum.c file
- Implements sophisticated chain management to maintain SP-GiST invariants during vacuum
- Uses critical sections and WAL logging for crash safety
- The `forPending` parameter prevents double-counting tuples in statistics
- Handles concurrent insertions through the pending list mechanism
- Performs extensive validation to detect chain corruption
- Uses four types of operations: dead tuple creation, placeholder insertion, tuple movement, and chain link updates
- The tuple movement is implemented by swapping line pointers for efficiency
- Includes comprehensive WAL logging for crash recovery
- Part of the SP-GiST vacuum subsystem designed to handle concurrent operations safely

## Simplified Source

```c
static void
vacuumLeafPage(spgBulkDeleteState *bds, Relation index, Buffer buffer,
               bool forPending)
{
    Page page = BufferGetPage(buffer);
    OffsetNumber toDead[MaxIndexTuplesPerPage];
    OffsetNumber toPlaceholder[MaxIndexTuplesPerPage];
    OffsetNumber moveSrc[MaxIndexTuplesPerPage];
    OffsetNumber moveDest[MaxIndexTuplesPerPage];
    OffsetNumber chainSrc[MaxIndexTuplesPerPage];
    OffsetNumber chainDest[MaxIndexTuplesPerPage];
    OffsetNumber predecessor[MaxIndexTuplesPerPage + 1];
    bool deletable[MaxIndexTuplesPerPage + 1];
    spgxlogVacuumLeaf xlrec;
    OffsetNumber i, max = PageGetMaxOffsetNumber(page);
    int nDeletable = 0;

    memset(predecessor, 0, sizeof(predecessor));
    memset(deletable, 0, sizeof(deletable));

    // Phase 1: Scan page, identify deletable tuples, build predecessor map
    for (i = FirstOffsetNumber; i <= max; i++)
    {
        SpGistLeafTuple lt = (SpGistLeafTuple) PageGetItem(page, PageGetItemId(page, i));

        if (lt->tupstate == SPGIST_LIVE)
        {
            // Check if tuple should be deleted
            if (bds->callback(&lt->heapPtr, bds->callback_state))
            {
                bds->stats->tuples_removed += 1;
                deletable[i] = true;
                nDeletable++;
            }
            else if (!forPending)
            {
                bds->stats->num_index_tuples += 1;
            }

            // Build chain predecessor map
            if (SGLT_GET_NEXTOFFSET(lt) != InvalidOffsetNumber)
                predecessor[SGLT_GET_NEXTOFFSET(lt)] = i;
        }
        else if (lt->tupstate == SPGIST_REDIRECT)
        {
            SpGistDeadTuple dt = (SpGistDeadTuple) lt;
            // Add concurrent redirects to pending list
            if (TransactionIdFollowsOrEquals(dt->xid, bds->myXmin))
                spgAddPendingTID(bds, &dt->pointer);
        }
    }

    if (nDeletable == 0)
        return;

    // Phase 2: Plan operations by processing tuple chains
    xlrec.nDead = xlrec.nPlaceholder = xlrec.nMove = xlrec.nChain = 0;

    for (i = FirstOffsetNumber; i <= max; i++)
    {
        SpGistLeafTuple head = (SpGistLeafTuple) PageGetItem(page, PageGetItemId(page, i));
        if (head->tupstate != SPGIST_LIVE || predecessor[i] != 0)
            continue;  // Skip non-live or non-head tuples

        // Process entire chain starting from this head
        bool interveningDeletable = false;
        OffsetNumber prevLive = deletable[i] ? InvalidOffsetNumber : i;
        OffsetNumber j = SGLT_GET_NEXTOFFSET(head);

        while (j != InvalidOffsetNumber)
        {
            SpGistLeafTuple lt = (SpGistLeafTuple) PageGetItem(page, PageGetItemId(page, j));

            if (deletable[j])
            {
                // Mark for placeholder replacement
                toPlaceholder[xlrec.nPlaceholder++] = j;
                interveningDeletable = true;
            }
            else if (prevLive == InvalidOffsetNumber)
            {
                // First live tuple in chain - move to head position
                moveSrc[xlrec.nMove] = j;
                moveDest[xlrec.nMove] = i;
                xlrec.nMove++;
                prevLive = i;
                interveningDeletable = false;
            }
            else
            {
                // Re-chain to previous live tuple if gap exists
                if (interveningDeletable)
                {
                    chainSrc[xlrec.nChain] = prevLive;
                    chainDest[xlrec.nChain] = j;
                    xlrec.nChain++;
                }
                prevLive = j;
                interveningDeletable = false;
            }
            j = SGLT_GET_NEXTOFFSET(lt);
        }

        // Handle chain completion
        if (prevLive == InvalidOffsetNumber)
            toDead[xlrec.nDead++] = i;  // Entire chain deletable
        else if (interveningDeletable)
        {
            // Close off chain after deletions
            chainSrc[xlrec.nChain] = prevLive;
            chainDest[xlrec.nChain] = InvalidOffsetNumber;
            xlrec.nChain++;
        }
    }

    // Phase 3: Execute planned operations
    START_CRIT_SECTION();

    // Apply all operations: dead, placeholder, move, chain updates
    spgPageIndexMultiDelete(&bds->spgstate, page, toDead, xlrec.nDead,
                            SPGIST_DEAD, SPGIST_DEAD,
                            InvalidBlockNumber, InvalidOffsetNumber);

    spgPageIndexMultiDelete(&bds->spgstate, page, toPlaceholder, xlrec.nPlaceholder,
                            SPGIST_PLACEHOLDER, SPGIST_PLACEHOLDER,
                            InvalidBlockNumber, InvalidOffsetNumber);

    // Move tuples by swapping line pointers
    for (i = 0; i < xlrec.nMove; i++)
    {
        ItemId idSrc = PageGetItemId(page, moveSrc[i]);
        ItemId idDest = PageGetItemId(page, moveDest[i]);
        ItemIdData tmp = *idSrc;
        *idSrc = *idDest;
        *idDest = tmp;
    }

    spgPageIndexMultiDelete(&bds->spgstate, page, moveSrc, xlrec.nMove,
                            SPGIST_PLACEHOLDER, SPGIST_PLACEHOLDER,
                            InvalidBlockNumber, InvalidOffsetNumber);

    // Update chain links
    for (i = 0; i < xlrec.nChain; i++)
    {
        SpGistLeafTuple lt = (SpGistLeafTuple) PageGetItem(page, PageGetItemId(page, chainSrc[i]));
        SGLT_SET_NEXTOFFSET(lt, chainDest[i]);
    }

    MarkBufferDirty(buffer);

    // WAL logging
    if (RelationNeedsWAL(index))
    {
        XLogRecPtr recptr;
        XLogBeginInsert();
        STORE_STATE(&bds->spgstate, xlrec.stateSrc);
        // Register all operation data with WAL
        XLogRegisterData((char *) &xlrec, SizeOfSpgxlogVacuumLeaf);
        XLogRegisterData((char *) toDead, sizeof(OffsetNumber) * xlrec.nDead);
        XLogRegisterData((char *) toPlaceholder, sizeof(OffsetNumber) * xlrec.nPlaceholder);
        XLogRegisterData((char *) moveSrc, sizeof(OffsetNumber) * xlrec.nMove);
        XLogRegisterData((char *) moveDest, sizeof(OffsetNumber) * xlrec.nMove);
        XLogRegisterData((char *) chainSrc, sizeof(OffsetNumber) * xlrec.nChain);
        XLogRegisterData((char *) chainDest, sizeof(OffsetNumber) * xlrec.nChain);
        XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);
        recptr = XLogInsert(RM_SPGIST_ID, XLOG_SPGIST_VACUUM_LEAF);
        PageSetLSN(page, recptr);
    }

    END_CRIT_SECTION();
}
```