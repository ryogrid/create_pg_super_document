# _bt_newlevel

## Location
[src/backend/access/nbtree/nbtinsert.c:2444-2629](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtinsert.c#L2444-L2629)

## Overview
Creates a new root level above the existing root page when a B-tree root split occurs, establishing a new tree level with downlink pointers to the split pages.

## Definition

```c
static Buffer
_bt_newlevel(Relation rel, Relation heaprel, Buffer lbuf, Buffer rbuf)
```
## Detailed Description
The  function is called during B-tree insertion when the root page needs to be split, requiring the creation of a new root page at a higher level. This operation is critical for maintaining B-tree balance and ensuring logarithmic search performance as the tree grows.

The function performs several key operations:
1. Allocates a new page to serve as the new root
2. Creates downlink index tuples pointing to the old root (left child) and its new sibling (right child)
3. Updates the B-tree metadata to reflect the new root and increased tree level
4. Handles WAL logging for crash recovery consistency
5. Uses critical sections to ensure atomicity of the multi-page update

The operation is designed to be deadlock-free by following a strict locking order: writers lock the root before the metadata page, while readers release metadata locks before attempting root locks.

## Parameters / Member Variables
- `rel`: The index relation being modified
- `heaprel`: The corresponding heap relation (used for space allocation)
- `lbuf`: Buffer containing the old root page (left child after split)
- `rbuf`: Buffer containing the new sibling page (right child after split)
## Dependencies
- Functions called/Symbols referenced:
  - : Allocates a new B-tree page
  - : Acquires a buffer for a specific page
  - : Updates metadata page format if needed
  - : Adds index tuples to the new root page
  - : Sets the downlink pointer in index tuples
  - : Records WAL entry for crash recovery
- Called from (representative examples):
  - : When inserting a new key requires root split

## Notes and Other Information
- The function operates within a critical section to ensure atomicity across multiple page updates
- Creates a "minus infinity" key for the left child downlink, ensuring it's less than any real key
- The right child downlink uses the high key from the original root page
- Updates both the regular root/level and fast root/level metadata fields
- Handles both old and new metadata page formats via version checking
- Returns the new root buffer, which the caller must unlock and unpin along with the child buffers

## Simplified Source

```c
static Buffer
_bt_newlevel(Relation rel, Relation heaprel, Buffer lbuf, Buffer rbuf)
{
    Buffer rootbuf, metabuf;
    Page lpage, rootpage, metapg;
    BlockNumber lbkno, rbkno, rootblknum;
    BTPageOpaque rootopaque, lopaque;
    IndexTuple left_item, right_item;
    BTMetaPageData *metad;

    // Get page info from left buffer (old root)
    lbkno = BufferGetBlockNumber(lbuf);
    rbkno = BufferGetBlockNumber(rbuf);
    lpage = BufferGetPage(lbuf);
    lopaque = BTPageGetOpaque(lpage);

    // Allocate new root page and get metadata
    rootbuf = _bt_allocbuf(rel, heaprel);
    rootpage = BufferGetPage(rootbuf);
    rootblknum = BufferGetBlockNumber(rootbuf);

    metabuf = _bt_getbuf(rel, BTREE_METAPAGE, BT_WRITE);
    metapg = BufferGetPage(metabuf);
    metad = BTPageGetMeta(metapg);

    // Create downlink items for left and right children
    // Left item: "minus infinity" key pointing to old root
    left_item = (IndexTuple) palloc(sizeof(IndexTupleData));
    left_item->t_info = sizeof(IndexTupleData);
    BTreeTupleSetDownLink(left_item, lbkno);
    BTreeTupleSetNAtts(left_item, 0, false);

    // Right item: copy high key from left page, point to right child
    ItemId itemid = PageGetItemId(lpage, P_HIKEY);
    right_item = CopyIndexTuple((IndexTuple) PageGetItem(lpage, itemid));
    BTreeTupleSetDownLink(right_item, rbkno);

    START_CRIT_SECTION();

    // Upgrade metapage if needed
    if (metad->btm_version < BTREE_NOVAC_VERSION)
        _bt_upgrademetapage(metapg);

    // Initialize new root page
    rootopaque = BTPageGetOpaque(rootpage);
    rootopaque->btpo_prev = rootopaque->btpo_next = P_NONE;
    rootopaque->btpo_flags = BTP_ROOT;
    rootopaque->btpo_level = BTPageGetOpaque(lpage)->btpo_level + 1;
    rootopaque->btpo_cycleid = 0;

    // Update metadata to point to new root
    metad->btm_root = rootblknum;
    metad->btm_level = rootopaque->btpo_level;
    metad->btm_fastroot = rootblknum;
    metad->btm_fastlevel = rootopaque->btpo_level;

    // Insert items into new root page (left item in P_HIKEY, right in P_FIRSTKEY)
    PageAddItem(rootpage, (Item) left_item, sizeof(IndexTupleData), P_HIKEY, false, false);
    PageAddItem(rootpage, (Item) right_item, ItemIdGetLength(itemid), P_FIRSTKEY, false, false);

    // Clear incomplete split flag on left child
    lopaque->btpo_flags &= ~BTP_INCOMPLETE_SPLIT;

    MarkBufferDirty(lbuf);
    MarkBufferDirty(rootbuf);
    MarkBufferDirty(metabuf);

    // WAL logging for crash recovery
    if (RelationNeedsWAL(rel)) {
        // Record new root creation with metadata update
        xl_btree_newroot xlrec;
        xl_btree_metadata md;

        xlrec.rootblk = rootblknum;
        xlrec.level = metad->btm_level;

        // Log complete metadata state
        md.version = metad->btm_version;
        md.root = rootblknum;
        md.level = metad->btm_level;
        md.fastroot = rootblknum;
        md.fastlevel = metad->btm_level;

        XLogBeginInsert();
        XLogRegisterData((char *) &xlrec, SizeOfBtreeNewroot);
        XLogRegisterBuffer(0, rootbuf, REGBUF_WILL_INIT);
        XLogRegisterBuffer(1, lbuf, REGBUF_STANDARD);
        XLogRegisterBuffer(2, metabuf, REGBUF_WILL_INIT | REGBUF_STANDARD);
        XLogRegisterBufData(2, (char *) &md, sizeof(xl_btree_metadata));

        XLogRecPtr recptr = XLogInsert(RM_BTREE_ID, XLOG_BTREE_NEWROOT);
        PageSetLSN(lpage, recptr);
        PageSetLSN(rootpage, recptr);
        PageSetLSN(metapg, recptr);
    }

    END_CRIT_SECTION();

    _bt_relbuf(rel, metabuf);
    pfree(left_item);
    pfree(right_item);

    return rootbuf;
}
```