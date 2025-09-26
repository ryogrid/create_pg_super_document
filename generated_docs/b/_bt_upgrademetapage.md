# _bt_upgrademetapage

## Location
[src/backend/access/nbtree/nbtpage.c:107-141](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtpage.c#L107-L141)

## Overview
_bt_upgrademetapage upgrades a B-tree metapage from an older format to version 3, which is the last version that can be updated without requiring a full REINDEX operation.

## Definition
void _bt_upgrademetapage(Page page)

## Detailed Description
This function performs an in-memory upgrade of a B-tree metapage from versions 2 or earlier to version 3 (BTREE_NOVAC_VERSION). The upgrade is necessary to support newer B-tree features and optimizations while maintaining backward compatibility for existing indexes. The function updates the version number, initializes new fields added in version 3 (cleanup statistics and allequalimage flag), and adjusts the page's pd_lower field to account for the larger metadata structure. The caller is responsible for proper locking and WAL logging of these changes. Note that upgrading to version 4 requires a full REINDEX operation.

## Parameters / Member Variables
- `page`: The metapage to upgrade (must contain a valid B-tree metapage of upgradable version)

## Dependencies
- Functions called/Symbols referenced:
  - BTPageGetMeta
  - BTPageGetOpaque
  - BTP_META
  - BTREE_NOVAC_VERSION
  - BTREE_MIN_VERSION
  - BTMetaPageData
  - BTPageOpaque
  - PageHeader
  - PG_USED_FOR_ASSERTS_ONLY
- Called from (representative examples):
  - _bt_insertonpg
  - _bt_newlevel
  - _bt_set_cleanup_info
  - _bt_getroot
  - _bt_unlink_halfdead_page

## Notes and Other Information
- Only upgrades metapages from versions >= BTREE_MIN_VERSION to version 3 (BTREE_NOVAC_VERSION)
- Initializes btm_last_cleanup_num_delpages to 0 and btm_last_cleanup_num_heap_tuples to -1.0
- Sets btm_allequalimage to false (only REINDEX can properly set this field)
- The upgrade is purely in-memory and requires proper WAL logging by the caller
- Version 4 upgrades require REINDEX due to more significant on-disk format changes
- Multiple assertions ensure the page is valid and upgradable before proceeding