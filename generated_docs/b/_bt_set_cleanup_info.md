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
  - _bt_getbuf
  - BufferGetPage
  - BTPageGetMeta
  - _bt_relbuf
  - _bt_unlockbuf
  - _bt_lockbuf
  - _bt_upgrademetapage
  - MarkBufferDirty
  - RelationNeedsWAL
  - XLogBeginInsert
  - XLogRegisterBuffer
  - XLogRegisterBufData
  - XLogInsert
  - PageSetLSN
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
  - BTMetaPageData
- Called from (representative examples):
  - btvacuumcleanup

## Notes and Other Information
- Upgrades read lock to write lock only when update is needed for efficiency
- Automatically upgrades metapage version if < BTREE_NOVAC_VERSION
- Sets btm_last_cleanup_num_heap_tuples to -1.0 (no longer used since PostgreSQL 14)
- Uses critical sections to ensure atomic updates with proper WAL logging
- Handles backward compatibility with repurposed btm_oldest_btpo_xact field
- Early return optimization when num_delpages hasn't changed (common case)
- WAL record includes complete metadata state for crash recovery