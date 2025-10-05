# _bt_gettrueroot

## Location
[src/backend/access/nbtree/nbtpage.c:580-674](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtpage.c#L580-L674)

## Overview
_bt_gettrueroot retrieves the actual root page of a B-tree index by following the true-root link rather than the fast-root link, bypassing performance optimizations for cases requiring the genuine root.

## Definition

```c
Buffer
_bt_gettrueroot(Relation rel)
```
## Detailed Description
This function is similar to _bt_getroot() in BT_READ mode but specifically follows the true-root (btm_root) link instead of the fast-root (btm_fastroot) link. Key characteristics:

1. **True Root Access**: Always accesses the actual root page rather than a potentially optimized fast-root page that might be at a different level due to deletions.

2. **Cache Invalidation**: Deliberately flushes any cached metadata since being here suggests the cache is likely out-of-date.

3. **Metadata Validation**: Performs thorough validation of the metadata page including magic number and version checks.

4. **Read-Only Operation**: Only performs read operations - does not create a root if none exists.

5. **Live Page Guarantee**: Ensures the returned page is live (not deleted) by traversing right-links if necessary.

The function is primarily used in specialized scenarios where the exact root page is needed, rather than the performance-optimized fast-root used by normal operations.

## Parameters / Member Variables
- `rel`: The B-tree index relation for which to retrieve the true root page
## Dependencies
- Functions called/Symbols referenced:
  - [_bt_getbuf](_bt_getbuf.md): Acquires buffer for specified block number  
  - [_bt_relbuf](_bt_relbuf.md): Releases buffer
  - [_bt_relandgetbuf](_bt_relandgetbuf.md): Releases and reacquires buffer for different page
  - BTPageGetOpaque: Gets B-tree page opaque area
  - BTPageGetMeta: Gets metadata from metapage
  - P_ISMETA: Checks if page is a metadata page
  - P_IGNORE: Checks if page should be ignored (deleted)
  - P_RIGHTMOST: Checks if page is rightmost on its level
- Called from (representative examples):
  - [_bt_get_endpoint](_bt_get_endpoint.md): When finding leftmost/rightmost pages needs true root

## Notes and Other Information
- Unlike _bt_getroot(), this function does not use cached metadata and actively flushes the cache
- Returns InvalidBuffer if no root page has been initialized yet
- Includes comprehensive error checking for corrupted indexes and version mismatches
- Used primarily in non-performance-critical paths where the exact root is required
- May return a page that gets split immediately after acquisition, but this is acceptable for current uses
- The function is located in src/backend/access/nbtree/nbtpage.c:580-674

## Simplified Source

```c
Buffer _bt_gettrueroot(Relation rel) {
    Buffer metabuf, rootbuf;
    Page metapg, rootpage;
    BTPageOpaque metaopaque, rootopaque;
    BlockNumber rootblkno;
    uint32 rootlevel;
    BTMetaPageData *metad;

    // Clear cached metadata since it's likely stale
    if (rel->rd_amcache) {
        pfree(rel->rd_amcache);
        rel->rd_amcache = NULL;
    }

    // Read metadata page and validate
    metabuf = _bt_getbuf(rel, BTREE_METAPAGE, BT_READ);
    metapg = BufferGetPage(metabuf);
    metaopaque = BTPageGetOpaque(metapg);
    metad = BTPageGetMeta(metapg);

    // Basic metadata validation
    if (!P_ISMETA(metaopaque) || metad->btm_magic != BTREE_MAGIC)
        ereport(ERROR, "index is not a btree");

    // Version compatibility check
    if (metad->btm_version < BTREE_MIN_VERSION ||
        metad->btm_version > BTREE_VERSION)
        ereport(ERROR, "version mismatch in index");

    // Check if root exists
    if (metad->btm_root == P_NONE) {
        _bt_relbuf(rel, metabuf);
        return InvalidBuffer;
    }

    // Get root page location and level
    rootblkno = metad->btm_root;
    rootlevel = metad->btm_level;
    rootbuf = metabuf;

    // Find live root page (skip deleted pages)
    for (;;) {
        rootbuf = _bt_relandgetbuf(rel, rootbuf, rootblkno, BT_READ);
        rootpage = BufferGetPage(rootbuf);
        rootopaque = BTPageGetOpaque(rootpage);

        if (!P_IGNORE(rootopaque))
            break;

        // Page is deleted, try next page
        if (P_RIGHTMOST(rootopaque))
            elog(ERROR, "no live root page found");
        rootblkno = rootopaque->btpo_next;
    }

    // Verify root page level matches metadata
    if (rootopaque->btpo_level != rootlevel)
        elog(ERROR, "root page level mismatch");

    return rootbuf;
}
```