# _bt_getroot

## Location
[src/backend/access/nbtree/nbtpage.c:344-579](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtpage.c#L344-L579)

## Overview
_bt_getroot retrieves the root page of a B-tree index, handling root page location changes and creating a new root page if necessary during write operations.

## Definition

```c
Buffer
_bt_getroot(Relation rel, Relation heaprel, int access)
```
## Detailed Description
This function is the primary interface for obtaining the root page of a B-tree index. It handles several complex scenarios:

1. **Cached Metadata Access**: First attempts to use cached metadata (rd_amcache) to quickly locate the root page, avoiding an extra buffer read in most cases.

2. **Dynamic Root Location**: Since B-tree root pages can move within the file due to splits and other operations, the function reads the current root location from the metadata page.

3. **Root Creation**: When no root exists yet and access is BT_WRITE, it creates a new root page that serves as both root and leaf initially.

4. **Fast Root Handling**: Returns a "fast root" page rather than insisting on the true root - this optimization handles cases where the root level has been reduced due to deletions.

5. **Concurrency Safety**: Includes proper locking protocols and handles race conditions during root creation.

The function guarantees to return a live (not deleted or half-dead) page that is pinned and read-locked, regardless of the access type requested.

## Parameters / Member Variables
- `rel`: The B-tree index relation being accessed
- `heaprel`: The heap relation associated with the index (required for BT_WRITE access, can be NULL for BT_READ)
- `access`: Access type - either BT_READ (read-only, won't create root) or BT_WRITE (may create root if needed)
## Dependencies
- Functions called/Symbols referenced:
  - [_bt_getbuf](_bt_getbuf.md): Acquires buffer for specified block number
  - [_bt_getmeta](_bt_getmeta.md): Gets metadata from metapage
  - [_bt_allocbuf](_bt_allocbuf.md): Allocates new buffer for page creation
  - [_bt_relbuf](_bt_relbuf.md): Releases buffer
  - [_bt_lockbuf](_bt_lockbuf.md)/_bt_unlockbuf: Buffer locking operations
  - [_bt_relandgetbuf](_bt_relandgetbuf.md): Releases and reacquires buffer for different page
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md): Gets block number from buffer
  - BTPageGetOpaque: Gets B-tree page opaque area
  - [XLogBeginInsert](../X/XLogBeginInsert.md)/XLogInsert: WAL logging functions
  - [XLogRegisterBuffer](../X/XLogRegisterBuffer.md)/XLogRegisterData: WAL record construction
- Called from (representative examples):
  - [_bt_search](_bt_search.md): Main B-tree search entry point
  - [_bt_get_endpoint](_bt_get_endpoint.md): Finds leftmost/rightmost leaf pages

## Notes and Other Information
- The returned root may be a "fast root" rather than the true root for performance reasons
- Function handles metadata caching to reduce buffer traffic
- Includes comprehensive WAL logging for root page creation
- Critical sections protect metadata updates during root creation
- Race condition handling ensures proper concurrent access during root initialization
- The function is located in src/backend/access/nbtree/nbtpage.c:344-579

## Simplified Source

```c
Buffer
_bt_getroot(Relation rel, Relation heaprel, int access)
{
    Buffer metabuf, rootbuf;
    Page rootpage, metapg;
    BTPageOpaque rootopaque;
    BlockNumber rootblkno;
    uint32 rootlevel;
    BTMetaPageData *metad;

    // Try to use cached metadata first (optimization)
    if (rel->rd_amcache != NULL) {
        metad = (BTMetaPageData *) rel->rd_amcache;
        rootblkno = metad->btm_fastroot;
        rootlevel = metad->btm_fastlevel;

        rootbuf = _bt_getbuf(rel, rootblkno, BT_READ);
        rootpage = BufferGetPage(rootbuf);
        rootopaque = BTPageGetOpaque(rootpage);

        // Validate cached page is still valid
        if (!P_IGNORE(rootopaque) &&
            rootopaque->btpo_level == rootlevel &&
            P_LEFTMOST(rootopaque) &&
            P_RIGHTMOST(rootopaque)) {
            return rootbuf; // Cache hit - return fast root
        }

        // Cache is stale, release buffer and clear cache
        _bt_relbuf(rel, rootbuf);
        if (rel->rd_amcache)
            pfree(rel->rd_amcache);
        rel->rd_amcache = NULL;
    }

    // Read metadata page
    metabuf = _bt_getbuf(rel, BTREE_METAPAGE, BT_READ);
    metad = _bt_getmeta(rel, metabuf);

    // Handle case where no root exists yet
    if (metad->btm_root == P_NONE) {
        if (access == BT_READ) {
            _bt_relbuf(rel, metabuf);
            return InvalidBuffer; // Don't create root for read access
        }

        // Upgrade to write lock for root creation
        _bt_unlockbuf(rel, metabuf);
        _bt_lockbuf(rel, metabuf, BT_WRITE);

        // Check for race condition
        if (metad->btm_root != P_NONE) {
            _bt_relbuf(rel, metabuf);
            return _bt_getroot(rel, heaprel, access); // Retry
        }

        // Create new root page (initially both root and leaf)
        rootbuf = _bt_allocbuf(rel, heaprel);
        rootblkno = BufferGetBlockNumber(rootbuf);
        rootpage = BufferGetPage(rootbuf);
        rootopaque = BTPageGetOpaque(rootpage);

        // Initialize root page
        rootopaque->btpo_prev = rootopaque->btpo_next = P_NONE;
        rootopaque->btpo_flags = (BTP_LEAF | BTP_ROOT);
        rootopaque->btpo_level = 0;
        rootopaque->btpo_cycleid = 0;

        metapg = BufferGetPage(metabuf);

        START_CRIT_SECTION();

        // Upgrade metapage if needed
        if (metad->btm_version < BTREE_NOVAC_VERSION)
            _bt_upgrademetapage(metapg);

        // Update metadata with new root
        metad->btm_root = rootblkno;
        metad->btm_level = 0;
        metad->btm_fastroot = rootblkno;
        metad->btm_fastlevel = 0;
        metad->btm_last_cleanup_num_delpages = 0;
        metad->btm_last_cleanup_num_heap_tuples = -1.0;

        MarkBufferDirty(rootbuf);
        MarkBufferDirty(metabuf);

        // WAL logging for new root
        if (RelationNeedsWAL(rel)) {
            xl_btree_newroot xlrec;
            xl_btree_metadata md;

            XLogBeginInsert();
            XLogRegisterBuffer(0, rootbuf, REGBUF_WILL_INIT);
            XLogRegisterBuffer(2, metabuf, REGBUF_WILL_INIT | REGBUF_STANDARD);

            md.version = metad->btm_version;
            md.root = rootblkno;
            md.level = 0;
            md.fastroot = rootblkno;
            md.fastlevel = 0;
            md.last_cleanup_num_delpages = 0;
            md.allequalimage = metad->btm_allequalimage;

            XLogRegisterBufData(2, (char *) &md, sizeof(xl_btree_metadata));

            xlrec.rootblk = rootblkno;
            xlrec.level = 0;
            XLogRegisterData((char *) &xlrec, SizeOfBtreeNewroot);

            XLogRecPtr recptr = XLogInsert(RM_BTREE_ID, XLOG_BTREE_NEWROOT);
            PageSetLSN(rootpage, recptr);
            PageSetLSN(metapg, recptr);
        }

        END_CRIT_SECTION();

        // Change root lock from write to read
        _bt_unlockbuf(rel, rootbuf);
        _bt_lockbuf(rel, rootbuf, BT_READ);
        _bt_relbuf(rel, metabuf);
    } else {
        // Root exists - get fast root location
        rootblkno = metad->btm_fastroot;
        rootlevel = metad->btm_fastlevel;

        // Cache metadata for next time
        rel->rd_amcache = MemoryContextAlloc(rel->rd_indexcxt,
                                            sizeof(BTMetaPageData));
        memcpy(rel->rd_amcache, metad, sizeof(BTMetaPageData));

        // Find a live root page
        rootbuf = metabuf; // Will be replaced by _bt_relandgetbuf

        for (;;) {
            rootbuf = _bt_relandgetbuf(rel, rootbuf, rootblkno, BT_READ);
            rootpage = BufferGetPage(rootbuf);
            rootopaque = BTPageGetOpaque(rootpage);

            if (!P_IGNORE(rootopaque))
                break; // Found live page

            // Dead page - try next page
            if (P_RIGHTMOST(rootopaque))
                elog(ERROR, "no live root page found in index \"%s\"",
                     RelationGetRelationName(rel));
            rootblkno = rootopaque->btpo_next;
        }

        // Validate root level matches expected
        if (rootopaque->btpo_level != rootlevel)
            elog(ERROR, "root page %u of index \"%s\" has level %u, expected %u",
                 rootblkno, RelationGetRelationName(rel),
                 rootopaque->btpo_level, rootlevel);
    }

    return rootbuf; // Root page pinned and read-locked
}
```