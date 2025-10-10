# _bt_killitems

## Location
[src/backend/access/nbtree/nbtutils.c:4171-4366](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtutils.c#L4171-L4366)

## Overview
Marks index tuples as dead (LP_DEAD) based on kill list information from index scan operations, optimizing future scans by marking tuples that reference deleted heap rows.

## Definition

```c
typedef struct BTOneVacInfo
{
	LockRelId	relid;			/* global identifier of an index */
	BTCycleId	cycleid;		/* cycle ID for its active VACUUM */
} BTOneVacInfo;
```
## Detailed Description
This function implements the "kill tuple" optimization for B-tree indexes, which marks index tuples as dead when the scan has determined that their corresponding heap tuples have been deleted. The function processes a list of killed items maintained in the scan state, matching them by heap TID to ensure correctness. It handles both regular tuples and posting list tuples (which contain multiple heap TIDs). The function includes sophisticated logic to handle concurrent modifications: if the page was pinned continuously since reading, no LSN check is needed; if the pin was dropped, it re-reads the page and verifies the LSN hasn't changed to ensure safety. When tuples are successfully marked dead, it sets the BTP_HAS_GARBAGE flag on the page to indicate cleanup is needed.

## Parameters / Member Variables
- : Index scan descriptor containing scan state and killed items list

## Dependencies
- Functions called/Symbols referenced:
  - [_bt_lockbuf](_bt_lockbuf.md)/_bt_unlockbuf
  - [_bt_getbuf](_bt_getbuf.md)/_bt_relbuf
  - [BufferGetLSNAtomic](../B/BufferGetLSNAtomic.md)
  - [PageGetItemId](../P/PageGetItemId.md)/PageGetItem
  - [BTreeTupleIsPosting](../B/BTreeTupleIsPosting.md)
  - [BTreeTupleGetNPosting](../B/BTreeTupleGetNPosting.md)/BTreeTupleGetPostingN
  - [ItemPointerEquals](../I/ItemPointerEquals.md)
  - ItemIdIsDead/ItemIdMarkDead
  - BTPageGetOpaque
  - [MarkBufferDirtyHint](../M/MarkBufferDirtyHint.md)
- Called from (representative examples):
  - [btrescan](btrescan.md)
  - [btendscan](btendscan.md)
  - [btrestrpos](btrestrpos.md)
  - [_bt_steppage](_bt_steppage.md)

## Notes and Other Information
- Critical optimization for reducing index bloat and improving scan performance
- Handles posting list tuples by checking all heap TIDs within the posting list
- Uses LSN checking to ensure safety when page pins were dropped between read and kill operations
- Sets BTP_HAS_GARBAGE flag to trigger eventual cleanup by VACUUM
- Only marks items as dead if they aren't already marked, avoiding redundant WAL logging
- Part of PostgreSQL's B-tree maintenance and optimization system
- Located in src/backend/access/nbtree/nbtutils.c:4171-4366

## Simplified Source

```c
void
_bt_killitems(IndexScanDesc scan)
{
    BTScanOpaque so = (BTScanOpaque) scan->opaque;
    Page page;
    BTPageOpaque opaque;
    OffsetNumber minoff, maxoff;
    int i, numKilled = so->numKilled;
    bool killedsomething = false;
    bool droppedpin;
    Buffer buf;

    // Reset scan state
    so->numKilled = 0;

    // Get and lock the page
    if (BTScanPosIsPinned(so->currPos))
    {
        // Page still pinned - just lock it
        droppedpin = false;
        buf = so->currPos.buf;
        _bt_lockbuf(scan->indexRelation, buf, BT_READ);
    }
    else
    {
        // Re-read page and verify it hasn't been modified
        droppedpin = true;
        buf = _bt_getbuf(scan->indexRelation, so->currPos.currPage, BT_READ);

        XLogRecPtr latestlsn = BufferGetLSNAtomic(buf);
        if (so->currPos.lsn != latestlsn)
        {
            // Page modified while unpinned - unsafe to kill items
            _bt_relbuf(scan->indexRelation, buf);
            return;
        }
    }

    page = BufferGetPage(buf);
    opaque = BTPageGetOpaque(page);
    minoff = P_FIRSTDATAKEY(opaque);
    maxoff = PageGetMaxOffsetNumber(page);

    // Process each killed item
    for (i = 0; i < numKilled; i++)
    {
        int itemIndex = so->killedItems[i];
        BTScanPosItem *kitem = &so->currPos.items[itemIndex];
        OffsetNumber offnum = kitem->indexOffset;

        if (offnum < minoff)
            continue;

        // Search for matching tuple by heap TID
        while (offnum <= maxoff)
        {
            ItemId iid = PageGetItemId(page, offnum);
            IndexTuple ituple = (IndexTuple) PageGetItem(page, iid);
            bool killtuple = false;

            if (BTreeTupleIsPosting(ituple))
            {
                // Handle posting list tuples (multiple heap TIDs)
                int nposting = BTreeTupleGetNPosting(ituple);
                int j;

                for (j = 0; j < nposting; j++)
                {
                    ItemPointer item = BTreeTupleGetPostingN(ituple, j);
                    if (!ItemPointerEquals(item, &kitem->heapTid))
                        break;

                    // Advance to next killed item if available
                    if (i + 1 < numKilled)
                    {
                        i++;
                        kitem = &so->currPos.items[so->killedItems[i]];
                    }
                }

                // Kill tuple if all heap TIDs matched
                if (j == nposting)
                    killtuple = true;
            }
            else if (ItemPointerEquals(&ituple->t_tid, &kitem->heapTid))
            {
                // Regular tuple with matching heap TID
                killtuple = true;
            }

            // Mark item as dead if not already dead
            if (killtuple && !ItemIdIsDead(iid))
            {
                ItemIdMarkDead(iid);
                killedsomething = true;
                break;
            }
            offnum = OffsetNumberNext(offnum);
        }
    }

    // Set page-level garbage flag and mark buffer dirty
    if (killedsomething)
    {
        opaque->btpo_flags |= BTP_HAS_GARBAGE;
        MarkBufferDirtyHint(buf, true);
    }

    // Release lock/buffer
    if (!droppedpin)
        _bt_unlockbuf(scan->indexRelation, buf);
    else
        _bt_relbuf(scan->indexRelation, buf);
}
```