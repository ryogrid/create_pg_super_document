# _bt_set_cleanup_info

## Location
[src/backend/access/nbtree/nbtpage.c:232-343](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtpage.c#L232-L343)

## Overview
_bt_set_cleanup_info updates the B-tree metapage with cleanup statistics at the end of btvacuumcleanup operations, including the number of deleted pages and performing version upgrades if necessary.

## Definition
void _bt_set_cleanup_info(Relation rel, BlockNumber num_delpages)

## Detailed Description
This function is called at the end of btvacuumcleanup to update the metapage with finalized cleanup statistics. It first checks if an update is actually needed by comparing the current num_delpages with the stored value. If an update is required, it upgrades the lock from read to write, potentially upgrades the metapage version using _bt_upgrademetapage if needed, and updates the cleanup-related fields. The function handles WAL logging when necessary and operates within a critical section to ensure atomicity. Historical note: the btm_last_cleanup_num_delpages field was repurposed from btm_oldest_btpo_xact when 64-bit XIDs were introduced, and btm_last_cleanup_num_heap_tuples is no longer used as of PostgreSQL 14.

## Parameters / Member Variables
- `rel`: The B-tree index relation to update
- `num_delpages`: The number of deleted pages found during cleanup

## Dependencies
- Functions called/Symbols referenced:
  - [_bt_getbuf](_bt_getbuf.md)
  - [BufferGetPage](../B/BufferGetPage.md)
  - BTPageGetMeta
  - [_bt_relbuf](_bt_relbuf.md)
  - [_bt_unlockbuf](_bt_unlockbuf.md)
  - [_bt_lockbuf](_bt_lockbuf.md)
  - [_bt_upgrademetapage](_bt_upgrademetapage.md)
  - [MarkBufferDirty](../M/MarkBufferDirty.md)
  - RelationNeedsWAL
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogRegisterBuffer](../X/XLogRegisterBuffer.md)
  - [XLogRegisterBufData](../X/XLogRegisterBufData.md)
  - [XLogInsert](../X/XLogInsert.md)
  - [PageSetLSN](../P/PageSetLSN.md)
  - START_CRIT_SECTION
  - END_CRIT_SECTION
  - BTREE_METAPAGE
  - BT_READ
  - BT_WRITE
  - BTREE_NOVAC_VERSION
  - REGBUF_WILL_INIT
  - REGBUF_STANDARD
  - XLOG_BTREE_META_CLEANUP
  - xl_btree_metadata
  - [BTMetaPageData](../B/BTMetaPageData.md)
- Called from (representative examples):
  - [btvacuumcleanup](btvacuumcleanup.md)

## Notes and Other Information
- Upgrades read lock to write lock only when update is needed for efficiency
- Automatically upgrades metapage version if < BTREE_NOVAC_VERSION
- Sets btm_last_cleanup_num_heap_tuples to -1.0 (no longer used since PostgreSQL 14)
- Uses critical sections to ensure atomic updates with proper WAL logging
- Handles backward compatibility with repurposed btm_oldest_btpo_xact field
- Early return optimization when num_delpages hasn't changed (common case)
- WAL record includes complete metadata state for crash recovery

## Simplified Source

```c
void
_bt_set_cleanup_info(Relation rel, BlockNumber num_delpages)
{
    Buffer metabuf;
    Page metapg;
    BTMetaPageData *metad;

    // Get metapage with read lock first
    metabuf = _bt_getbuf(rel, BTREE_METAPAGE, BT_READ);
    metapg = BufferGetPage(metabuf);
    metad = BTPageGetMeta(metapg);

    // Early exit if no update needed (common optimization)
    if (metad->btm_version >= BTREE_NOVAC_VERSION &&
        metad->btm_last_cleanup_num_delpages == num_delpages) {
        _bt_relbuf(rel, metabuf);
        return;
    }

    // Upgrade to write lock for actual update
    _bt_unlockbuf(rel, metabuf);
    _bt_lockbuf(rel, metabuf, BT_WRITE);

    START_CRIT_SECTION();

    // Upgrade metapage version if needed
    if (metad->btm_version < BTREE_NOVAC_VERSION)
        _bt_upgrademetapage(metapg);

    // Update cleanup statistics
    metad->btm_last_cleanup_num_delpages = num_delpages;
    metad->btm_last_cleanup_num_heap_tuples = -1.0;  // No longer used
    MarkBufferDirty(metabuf);

    // WAL logging for crash recovery
    if (RelationNeedsWAL(rel)) {
        xl_btree_metadata md;
        XLogRecPtr recptr;

        // Record complete metadata state
        md.version = metad->btm_version;
        md.root = metad->btm_root;
        md.level = metad->btm_level;
        md.fastroot = metad->btm_fastroot;
        md.fastlevel = metad->btm_fastlevel;
        md.last_cleanup_num_delpages = num_delpages;
        md.allequalimage = metad->btm_allequalimage;

        XLogBeginInsert();
        XLogRegisterBuffer(0, metabuf, REGBUF_WILL_INIT | REGBUF_STANDARD);
        XLogRegisterBufData(0, (char *) &md, sizeof(xl_btree_metadata));

        recptr = XLogInsert(RM_BTREE_ID, XLOG_BTREE_META_CLEANUP);
        PageSetLSN(metapg, recptr);
    }

    END_CRIT_SECTION();

    _bt_relbuf(rel, metabuf);
}
```