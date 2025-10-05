# _bt_stepright

## Location
[src/backend/access/nbtree/nbtinsert.c:1027-1104](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtinsert.c#L1027-L1104)

## Overview
Steps right to the next non-dead leaf page during insertion while maintaining proper write-lock ordering to prevent concurrency issues.

## Definition

```c
static void
_bt_stepright(Relation rel, Relation heaprel, BTInsertState insertstate,
			  BTStack stack)
```
## Detailed Description
The  function moves the insertion context from the current leaf page to the next suitable leaf page to the right. This operation is more complex than a simple search movement because it must maintain strict locking protocols to ensure that concurrent uniqueness checking operations can see insertions correctly.

The function implements a crucial locking protocol: it acquires a write lock on the target page before releasing the write lock on the current page. This prevents other transactions' uniqueness scans from missing the insertion that's in progress. Without this careful ordering, a concurrent transaction could incorrectly conclude that a duplicate doesn't exist.

The function also handles special cases like incomplete page splits and dead/ignored pages, ensuring that the insertion proceeds to a valid, usable leaf page.

## Parameters / Member Variables
- `rel`: The B-tree index relation being operated on
- `heaprel`: The heap relation associated with the index (must not be NULL)
- `insertstate`: Current insertion state to be updated with new buffer
- `stack`: Search stack needed for potential split completion
## Dependencies
- Functions called/Symbols referenced:
  - [_bt_relandgetbuf](_bt_relandgetbuf.md): Releases current buffer and acquires new one with specified lock
  - [_bt_finish_split](_bt_finish_split.md): Completes any incomplete page splits encountered
  - [_bt_relbuf](_bt_relbuf.md): Releases buffer lock and pin
  - P_INCOMPLETE_SPLIT: Checks if page has incomplete split
  - P_IGNORE: Checks if page should be ignored (dead)
  - P_RIGHTMOST: Checks if page is rightmost in tree
- Called from (representative examples):
  - [_bt_findinsertloc](_bt_findinsertloc.md): When searching for optimal insertion location

## Notes and Other Information
- Updates insertstate->buf to point to the new buffer and invalidates cached bounds
- Maintains write lock on target page while releasing lock on source page for concurrency safety
- Handles incomplete page splits by completing them before proceeding
- Skips over ignored/dead pages to find next valid insertion target
- More aggressive locking than strictly necessary for non-unique indexes, but ensures correctness
- Will error if it encounters the rightmost page while looking for ignored pages
- Critical for maintaining consistency in concurrent unique index operations

## Simplified Source

```c
static void _bt_stepright(Relation rel, Relation heaprel, BTInsertState insertstate, BTStack stack) {
    Page page;
    BTPageOpaque opaque;
    Buffer rbuf;
    BlockNumber rblkno;

    page = BufferGetPage(insertstate->buf);
    opaque = BTPageGetOpaque(page);

    rbuf = InvalidBuffer;
    rblkno = opaque->btpo_next;

    // Find next suitable page
    for (;;) {
        // Get write lock on next page before releasing current
        rbuf = _bt_relandgetbuf(rel, rbuf, rblkno, BT_WRITE);
        page = BufferGetPage(rbuf);
        opaque = BTPageGetOpaque(page);

        // Handle incomplete splits
        if (P_INCOMPLETE_SPLIT(opaque)) {
            _bt_finish_split(rel, heaprel, rbuf, stack);
            rbuf = InvalidBuffer;
            continue;
        }

        // Found a good page
        if (!P_IGNORE(opaque))
            break;

        // Check for end of index
        if (P_RIGHTMOST(opaque))
            elog(ERROR, "fell off the end of index \"%s\"",
                 RelationGetRelationName(rel));

        rblkno = opaque->btpo_next;
    }

    // Update insertion state with new buffer
    _bt_relbuf(rel, insertstate->buf);
    insertstate->buf = rbuf;
    insertstate->bounds_valid = false;
}
```