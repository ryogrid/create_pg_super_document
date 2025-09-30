# _bt_getrootheight

## Location
[src/backend/access/nbtree/nbtpage.c:675-738](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtpage.c#L675-L738)

## Overview
_bt_getrootheight returns the height of the B-tree search tree by retrieving the fast root level, used primarily for query planner cost estimation.

## Definition

```c
int
_bt_getrootheight(Relation rel)
```
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
  - [_bt_getbuf](_bt_getbuf.md): Acquires buffer for metadata page
  - [_bt_getmeta](_bt_getmeta.md): Gets metadata from metapage
  - [_bt_relbuf](_bt_relbuf.md): Releases buffer
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md): Allocates memory for caching metadata
  - [BTMetaPageData](../B/BTMetaPageData.md): Metadata structure type
  - BTREE_METAPAGE: Metadata page constant
- Called from (representative examples):
  - [get_relation_info](../g/get_relation_info.md): Planner function gathering relation statistics
  - [_bt_insertonpg](_bt_insertonpg.md): Internal B-tree insertion logic

## Notes and Other Information
- Designed specifically for query planning cost estimation rather than exact operations
- Uses cached metadata aggressively to minimize I/O overhead
- Returns the fast root level rather than true root level for performance
- Empty indexes (no root page) return height 0
- The cached data includes validation assertions but tolerates slightly stale information
- The function is located in src/backend/access/nbtree/nbtpage.c:675-738

## Simplified Source

```c
int
_bt_getrootheight(Relation rel)
{
    BTMetaPageData *metad;

    // Check if metadata is already cached
    if (rel->rd_amcache == NULL) {
        Buffer metabuf;

        // Read metadata page
        metabuf = _bt_getbuf(rel, BTREE_METAPAGE, BT_READ);
        metad = _bt_getmeta(rel, metabuf);

        // Handle case where no root page exists yet
        if (metad->btm_root == P_NONE) {
            _bt_relbuf(rel, metabuf);
            return 0;  // Empty index
        }

        // Cache the metadata for future calls
        rel->rd_amcache = MemoryContextAlloc(rel->rd_indexcxt, sizeof(BTMetaPageData));
        memcpy(rel->rd_amcache, metad, sizeof(BTMetaPageData));
        _bt_relbuf(rel, metabuf);
    }

    // Use cached metadata
    metad = (BTMetaPageData *) rel->rd_amcache;

    // Basic validation assertions
    Assert(metad->btm_magic == BTREE_MAGIC);
    Assert(metad->btm_version >= BTREE_MIN_VERSION);
    Assert(metad->btm_version <= BTREE_VERSION);
    Assert(!metad->btm_allequalimage || metad->btm_version > BTREE_NOVAC_VERSION);
    Assert(metad->btm_fastroot != P_NONE);

    // Return the fast root level (tree height)
    return metad->btm_fastlevel;
}
```