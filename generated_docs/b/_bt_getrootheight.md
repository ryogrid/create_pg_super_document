# _bt_getrootheight

## Location
src/backend/access/nbtree/nbtpage.c: 675 - 738

## Overview
_bt_getrootheight returns the height of the B-tree search tree by retrieving the fast root level, used primarily for query planner cost estimation.

## Definition


## Detailed Description
This function provides the height (level) of the B-tree index, counting from zero at the leaf level. Key characteristics:

1. **Fast Root Level**: Returns the btm_fastlevel from metadata, representing the number of tree levels that would need to be descended through to start any index search.

2. **Planner Integration**: Primarily used by the PostgreSQL query planner for cost estimation purposes when evaluating different query execution strategies.

3. **Caching Strategy**: Uses and creates cached metadata when possible to avoid repeated metadata page reads, since exact precision isn't critical for cost estimation.

4. **Stale Data Tolerance**: Accepts slightly stale cached data as adequate for cost estimation, avoiding unnecessary cache invalidation.

5. **Zero Height Handling**: Returns 0 if no root page exists yet, indicating an empty index.

The function prioritizes performance over absolute accuracy since it's used for estimation rather than critical operations.

## Parameters / Member Variables
- : The B-tree index relation for which to determine the tree height

## Dependencies
- Functions called/Symbols referenced:
  - _bt_getbuf: Acquires buffer for metadata page
  - _bt_getmeta: Gets metadata from metapage
  - _bt_relbuf: Releases buffer
  - MemoryContextAlloc: Allocates memory for caching metadata
  - BTMetaPageData: Metadata structure type
  - BTREE_METAPAGE: Metadata page constant
- Called from (representative examples):
  - get_relation_info: Planner function gathering relation statistics
  - _bt_insertonpg: Internal B-tree insertion logic

## Notes and Other Information
- Designed specifically for query planning cost estimation rather than exact operations
- Uses cached metadata aggressively to minimize I/O overhead
- Returns the fast root level rather than true root level for performance
- Empty indexes (no root page) return height 0
- The cached data includes validation assertions but tolerates slightly stale information
- The function is located in src/backend/access/nbtree/nbtpage.c:675-738