# hash_xlog_init_bitmap_page

## Location
[src/backend/access/hash/hash_xlog.c:63-124](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hash_xlog.c#L63-L124)

## Overview
Replays the initialization of a hash index bitmap page during WAL recovery, including updating the metapage to register the new bitmap page.

## Definition

```c
static void
hash_xlog_init_bitmap_page(XLogReaderState *record)
```
## Detailed Description
This function handles WAL replay for bitmap page initialization in hash indexes. Hash indexes use bitmap pages to track which overflow pages are available for allocation. During recovery, this function reconstructs both the bitmap page itself and updates the metapage to include the new bitmap page in its bitmap registry.

The function performs two main operations: first, it initializes the bitmap page buffer with the specified bitmap size; second, it updates the metapage's bitmap registry by adding an entry for the new bitmap page and incrementing the bitmap count. Special handling ensures init fork synchronization between shared buffers and disk state.

## Parameters / Member Variables  
- `*record`: XLogReaderState pointer containing the WAL record with bitmap initialization data including the bitmap size (bmsize)
## Dependencies
- Functions called/Symbols referenced:
  - [xl_hash_init_bitmap_page](../x/xl_hash_init_bitmap_page.md) (WAL record structure)
  - XLogRecGetData (extracts record data)
  - [XLogInitBufferForRedo](../X/XLogInitBufferForRedo.md) (initializes buffer for recovery)
  - [_hash_initbitmapbuffer](_hash_initbitmapbuffer.md) (initializes bitmap page buffer)
  - [XLogRecGetBlockTag](../X/XLogRecGetBlockTag.md) (gets block information)
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md) (reads metapage buffer for redo)
  - HashPageGetMeta (gets metapage metadata)
  - BLK_NEEDS_REDO (indicates block needs redo)
  - INIT_FORKNUM (fork number constant)
  - [FlushOneBuffer](../F/FlushOneBuffer.md) (flushes buffer to disk)
- Called from:
  - [hash_redo](hash_redo.md) (main hash WAL replay function)

## Notes and Other Information
- This is a static function used only within the hash WAL recovery subsystem
- The function handles two buffers: the bitmap page being initialized and the metapage being updated
- During normal operation, both pages would be locked simultaneously, but during replay this isn't necessary since the transaction hasn't committed yet
- Init fork handling ensures proper synchronization between shared buffers and disk for both the bitmap page and metapage
- The metapage's hashm_mapp array is updated to include the new bitmap page location, and hashm_nmaps is incremented