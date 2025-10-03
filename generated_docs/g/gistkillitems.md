# gistkillitems

## Location
[src/backend/access/gist/gistget.c:38-124](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistget.c#L38-L124)

## Overview
gistkillitems is a static function that marks index tuples as dead (LP_DEAD state) for items that an index scan caller has indicated were killed, implementing tuple visibility optimization in GiST indexes.

## Definition

```c
static void
gistkillitems(IndexScanDesc scan)
```
## Detailed Description
This function implements a performance optimization for GiST index scans by marking tuples as dead when the scan caller indicates they are no longer needed. The function performs several important safety checks:

1. **LSN Verification**: Re-reads the page and compares the current LSN with the stored LSN from the last read. If they differ, the page has been modified and it's unsafe to mark items as dead because the old entries might have been vacuumed and TIDs reused.

2. **Killed Items Processing**: If the page hasn't been modified, iterates through all items marked for killing and sets them to LP_DEAD state using ItemIdMarkDead().

3. **Page State Management**: If any items were killed, marks the page as having garbage and sets the buffer dirty hint for eventual cleanup.

This optimization helps reduce repeated visibility checks for tuples that are known to be dead, improving scan performance.

## Parameters / Member Variables
- : IndexScanDesc containing the index scan state, including the opaque GiST scan data with killed items information

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecPtrIsInvalid
  - [ReadBuffer](../R/ReadBuffer.md)
  - [gistcheckpage](gistcheckpage.md)
  - [BufferGetLSNAtomic](../B/BufferGetLSNAtomic.md)
  - GistPageIsLeaf
  - [PageGetItemId](../P/PageGetItemId.md)
  - ItemIdMarkDead
  - GistMarkPageHasGarbage
  - [MarkBufferDirtyHint](../M/MarkBufferDirtyHint.md)
- Called from:
  - [gistgettuple](gistgettuple.md)

## Notes and Other Information
- This is a static function, only accessible within the gistget.c file
- Requires that curBlkno, curPageLSN, and killedItems are properly set in the scan opaque data
- The LSN check is critical for correctness - without it, there's a risk of marking valid tuples as dead
- Always resets numKilled counter regardless of whether items were actually killed
- Only works on leaf pages (asserted with GistPageIsLeaf)

## Simplified Source

```c
static void gistkillitems(IndexScanDesc scan) {
    GISTScanOpaque so = (GISTScanOpaque) scan->opaque;
    Buffer buffer;
    Page page;
    bool killedsomething = false;

    // Read the page and acquire share lock
    buffer = ReadBuffer(scan->indexRelation, so->curBlkno);
    if (!BufferIsValid(buffer))
        return;

    LockBuffer(buffer, GIST_SHARE);
    gistcheckpage(scan->indexRelation, buffer);
    page = BufferGetPage(buffer);

    // Safety check: ensure page hasn't been modified since last read
    if (BufferGetLSNAtomic(buffer) != so->curPageLSN) {
        UnlockReleaseBuffer(buffer);
        so->numKilled = 0;
        return;
    }

    // Mark all killed items as dead
    for (int i = 0; i < so->numKilled; i++) {
        OffsetNumber offnum = so->killedItems[i];
        ItemId iid = PageGetItemId(page, offnum);
        ItemIdMarkDead(iid);
        killedsomething = true;
    }

    // Update page state if items were killed
    if (killedsomething) {
        GistMarkPageHasGarbage(page);
        MarkBufferDirtyHint(buffer, true);
    }

    UnlockReleaseBuffer(buffer);

    // Reset killed items counter
    so->numKilled = 0;
}
```