# _bt_vacuum_needs_cleanup

## Location
[src/backend/access/nbtree/nbtpage.c:179-231](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtpage.c#L179-L231)

## Overview
_bt_vacuum_needs_cleanup determines whether a B-tree index requires cleanup processing during vacuum operations when no index tuples were deleted.

## Definition
bool _bt_vacuum_needs_cleanup(Relation rel)

## Detailed Description
This function is called by btvacuumcleanup when btbulkdelete was never invoked because no index tuples needed deletion. It examines the B-tree metapage to decide whether cleanup processing is necessary based on two key conditions: (1) if the metapage version is older than BTREE_NOVAC_VERSION and requires an upgrade, or (2) if the number of previously deleted pages exceeds 5% of the total index size. The function deliberately avoids using cached metapage data to ensure it gets current information. When cleanup is needed, it allows the vacuum process to potentially recycle deleted pages and update metapage statistics, improving space utilization and performance.

## Parameters / Member Variables
- `rel`: The B-tree index relation to check for cleanup needs

## Dependencies
- Functions called/Symbols referenced:
  - _bt_getbuf
  - BufferGetPage
  - BTPageGetMeta
  - _bt_relbuf
  - RelationGetNumberOfBlocks
  - BTREE_METAPAGE
  - BT_READ
  - BTREE_NOVAC_VERSION
  - BTMetaPageData
- Called from (representative examples):
  - btvacuumcleanup

## Notes and Other Information
- Deliberately avoids cached metapage data to ensure current information
- Returns true if metapage version < BTREE_NOVAC_VERSION (requires upgrade)
- Returns true if deleted pages > 5% of total index size (prev_num_delpages > total_blocks/20)
- The 5% threshold is a heuristic to balance cleanup frequency with performance
- Cleanup allows recycling deleted pages and updating metapage statistics
- Used only when btbulkdelete was not called (no tuples needed deletion)
- See nbtree/README for details on deleted page management and FSM placement