# _bt_allocbuf

## Location
[src/backend/access/nbtree/nbtpage.c:869-1002](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtpage.c#L869-L1002)

## Overview
_bt_allocbuf allocates a new block/page for a B-tree index, either by reusing a page from the Free Space Map (FSM) or by extending the relation with a new page.

## Definition
```c
Buffer _bt_allocbuf(Relation rel, Relation heaprel)
```

## Detailed Description
This function implements a sophisticated page allocation strategy for B-tree indexes. It first attempts to reuse pages from the FSM, but includes careful safeguards to handle race conditions and deadlocks. The function uses conditional locking to avoid deadlocks when trying to reuse FSM pages, and includes proper WAL logging for Hot Standby conflict detection when reusing pages.

The allocation process follows these steps:
1. Query FSM for potentially free pages
2. Attempt conditional locking on reported pages to avoid deadlocks
3. Verify pages are actually recyclable using BTPageIsRecyclable
4. Generate WAL records for Hot Standby conflict detection if needed
5. Fall back to extending the relation if no suitable pages are found

The function handles edge cases like all-zeroes pages (from crashed backends) and ensures proper initialization of allocated pages.

## Parameters / Member Variables
- `rel`: The B-tree index relation to allocate a page for
- `heaprel`: The associated heap relation (required for generating snapshotConflictHorizon for Hot Standby safety)

## Dependencies
- Functions called/Symbols referenced:
  - [GetFreeIndexPage](../G/GetFreeIndexPage.md) (queries FSM for free pages)
  - [ReadBuffer](../R/ReadBuffer.md) (reads candidate pages into buffer pool)
  - [_bt_conditionallockbuf](_bt_conditionallockbuf.md) (attempts conditional locking to avoid deadlocks)
  - [PageIsNew](../P/PageIsNew.md) (checks for all-zeroes pages)
  - [BTPageIsRecyclable](../B/BTPageIsRecyclable.md) (verifies page can be safely reused)
  - [XLogBeginInsert](../X/XLogBeginInsert.md), XLogRegisterData, XLogInsert (WAL logging for page reuse)
  - [ExtendBufferedRel](../E/ExtendBufferedRel.md) (extends relation when no reusable pages available)
  - [_bt_pageinit](_bt_pageinit.md) (initializes allocated pages)
  - [_bt_relbuf](_bt_relbuf.md) (releases non-suitable pages)
- Called from (representative examples):
  - [_bt_split](_bt_split.md) (during page splitting operations)
  - [_bt_newlevel](_bt_newlevel.md) (when creating new B-tree levels)
  - [_bt_getroot](_bt_getroot.md) (when allocating initial root page)

## Notes and Other Information
- Uses conditional locking strategy to prevent deadlocks with concurrent operations
- Includes sophisticated handling of race conditions with FSM and VACUUM
- Generates WAL records for Hot Standby conflict detection when reusing pages
- Falls back to relation extension when FSM pages aren't suitable
- Handles edge case of all-zeroes pages from backend crashes
- Returns a write-locked buffer containing an initialized, empty B-tree page
- Located in src/backend/access/nbtree/nbtpage.c:869-1002

## Simplified Source

```c
Buffer _bt_allocbuf(Relation rel, Relation heaprel) {
    Buffer buf;
    BlockNumber blkno;
    Page page;

    Assert(heaprel != NULL);

    // Try to find a reusable page from Free Space Map
    for (;;) {
        blkno = GetFreeIndexPage(rel);
        if (blkno == InvalidBlockNumber)
            break;  // No more FSM pages to try

        buf = ReadBuffer(rel, blkno);

        // Use conditional locking to avoid deadlocks
        if (_bt_conditionallockbuf(rel, buf)) {
            page = BufferGetPage(buf);

            // Handle all-zeroes pages from crashed backends
            if (PageIsNew(page)) {
                _bt_pageinit(page, BufferGetPageSize(buf));
                return buf;
            }

            // Check if page can be safely recycled
            if (BTPageIsRecyclable(page, heaprel)) {
                // Generate WAL record for Hot Standby conflict detection
                if (RelationNeedsWAL(rel) && XLogStandbyInfoActive()) {
                    xl_btree_reuse_page xlrec_reuse;
                    xlrec_reuse.locator = rel->rd_locator;
                    xlrec_reuse.block = blkno;
                    xlrec_reuse.snapshotConflictHorizon = BTPageGetDeleteXid(page);
                    xlrec_reuse.isCatalogRel =
                        RelationIsAccessibleInLogicalDecoding(heaprel);

                    XLogBeginInsert();
                    XLogRegisterData((char *) &xlrec_reuse, SizeOfBtreeReusePage);
                    XLogInsert(RM_BTREE_ID, XLOG_BTREE_REUSE_PAGE);
                }

                // Page is suitable for reuse
                _bt_pageinit(page, BufferGetPageSize(buf));
                return buf;
            }

            // Page not recyclable, release and try next
            _bt_relbuf(rel, buf);
        } else {
            // Couldn't lock page, release and try next
            ReleaseBuffer(buf);
        }
    }

    // No suitable FSM pages found, extend the relation
    buf = ExtendBufferedRel(BMR_REL(rel), MAIN_FORKNUM, NULL, EB_LOCK_FIRST);
    if (!RelationUsesLocalBuffers(rel))
        VALGRIND_MAKE_MEM_DEFINED(BufferGetPage(buf), BLCKSZ);

    // Initialize the new page
    page = BufferGetPage(buf);
    Assert(PageIsNew(page));
    _bt_pageinit(page, BufferGetPageSize(buf));

    return buf;
}
```