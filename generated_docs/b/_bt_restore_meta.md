# _bt_restore_meta

## Location
[src/backend/access/nbtree/nbtxlog.c:82-138](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtxlog.c#L82-L138)

## Overview
Restores the B-tree metapage from WAL record data during recovery operations.

## Definition
```c
static void _bt_restore_meta(XLogReaderState *record, uint8 block_id)
```

## Detailed Description
This function is responsible for reconstructing the B-tree metapage during WAL replay. The metapage contains critical metadata about the B-tree index structure, including the root page location, tree level, fast root information, and version details.

The function initializes a fresh metapage, extracts metadata from the WAL record, and populates all the essential metapage fields. It ensures proper page initialization, sets the appropriate page opaque data to mark it as a metapage, and carefully manages the page's lower boundary to prevent metadata loss during page compression.

The restoration process includes validation checks to ensure the data integrity and compatibility with the current B-tree version requirements.

## Parameters / Member Variables
- `record`: XLogReaderState containing the WAL record data for the metapage restoration
- `block_id`: Identifier of the block within the WAL record that contains the metapage data

## Dependencies
- Functions called/Symbols referenced:
  - [XLogInitBufferForRedo](../X/XLogInitBufferForRedo.md)
  - [XLogRecGetBlockData](../X/XLogRecGetBlockData.md)
  - [_bt_pageinit](_bt_pageinit.md)
  - BTPageGetMeta
  - BTPageGetOpaque
  - [BufferGetPage](../B/BufferGetPage.md)
  - [BufferGetPageSize](../B/BufferGetPageSize.md)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
  - [PageSetLSN](../P/PageSetLSN.md)
  - [MarkBufferDirty](../M/MarkBufferDirty.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
- Data types used:
  - [BTMetaPageData](../B/BTMetaPageData.md)
  - BTPageOpaque
  - xl_btree_metadata
  - PageHeader
- Constants used:
  - BTREE_MAGIC
  - BTREE_METAPAGE
  - BTREE_NOVAC_VERSION
  - BTP_META
- Called from (representative examples):
  - [btree_xlog_insert](btree_xlog_insert.md)
  - [btree_xlog_unlink_page](btree_xlog_unlink_page.md)
  - [btree_xlog_newroot](btree_xlog_newroot.md)
  - [btree_redo](btree_redo.md)

## Notes and Other Information
- This is a static function used internally within nbtxlog.c for B-tree WAL recovery
- Includes assertion checks to validate that the incoming data matches expected xl_btree_metadata structure size
- Ensures the buffer corresponds to the actual metapage (block number BTREE_METAPAGE)
- Sets pd_lower carefully to prevent metadata loss during potential page compression by xlog.c
- Initializes btm_last_cleanup_num_heap_tuples to -1.0 as a default value
- Requires B-tree version to be at least BTREE_NOVAC_VERSION for proper operation
- Properly manages buffer lifecycle with MarkBufferDirty and UnlockReleaseBuffer calls

## Simplified Source

```c
static void _bt_restore_meta(XLogReaderState *record, uint8 block_id)
{
    XLogRecPtr lsn = record->EndRecPtr;
    Buffer metabuf;
    Page metapg;
    BTMetaPageData *md;
    BTPageOpaque pageop;
    xl_btree_metadata *xlrec;
    char *ptr;
    Size len;

    // Initialize buffer and get WAL record data
    metabuf = XLogInitBufferForRedo(record, block_id);
    ptr = XLogRecGetBlockData(record, block_id, &len);

    // Validate the record
    Assert(len == sizeof(xl_btree_metadata));
    Assert(BufferGetBlockNumber(metabuf) == BTREE_METAPAGE);

    xlrec = (xl_btree_metadata *) ptr;
    metapg = BufferGetPage(metabuf);

    // Initialize the metapage
    _bt_pageinit(metapg, BufferGetPageSize(metabuf));

    // Restore metadata from WAL record
    md = BTPageGetMeta(metapg);
    md->btm_magic = BTREE_MAGIC;
    md->btm_version = xlrec->version;
    md->btm_root = xlrec->root;
    md->btm_level = xlrec->level;
    md->btm_fastroot = xlrec->fastroot;
    md->btm_fastlevel = xlrec->fastlevel;
    md->btm_last_cleanup_num_delpages = xlrec->last_cleanup_num_delpages;
    md->btm_last_cleanup_num_heap_tuples = -1.0;  // Default value
    md->btm_allequalimage = xlrec->allequalimage;

    // Set page opaque data to mark as metapage
    pageop = BTPageGetOpaque(metapg);
    pageop->btpo_flags = BTP_META;

    // Set page boundary to prevent metadata loss during compression
    ((PageHeader) metapg)->pd_lower =
        ((char *) md + sizeof(BTMetaPageData)) - (char *) metapg;

    // Finalize the restoration
    PageSetLSN(metapg, lsn);
    MarkBufferDirty(metabuf);
    UnlockReleaseBuffer(metabuf);
}
```