# hash_xlog_add_ovfl_page

## Location
[src/backend/access/hash/hash_xlog.c:173-310](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hash_xlog.c#L173-L310)

## Overview
Replays the addition of an overflow page to a hash index during WAL recovery, managing the complex process of linking pages and updating bitmap and metapage structures.

## Definition

```c
static void
hash_xlog_add_ovfl_page(XLogReaderState *record)
```
## Detailed Description
This function handles WAL replay for adding overflow pages to hash indexes when bucket pages become full. Hash indexes use overflow pages to store additional tuples when the primary bucket page cannot accommodate more data. This operation involves multiple components: creating the new overflow page, linking it to the existing page chain, updating bitmap pages to mark the page as allocated, potentially creating new bitmap pages if needed, and updating metapage statistics.

The function operates on up to 5 different buffers: the new overflow page (block 0), the left page that will point to it (block 1), an existing bitmap page (block 2), a potential new bitmap page (block 3), and the metapage (block 4). The function maintains proper page linkage by setting forward and backward pointers, updates bitmap allocation status, and manages metapage statistics including the first free overflow page pointer and overflow point counters.

## Parameters / Member Variables
- : XLogReaderState pointer containing the WAL record with overflow page data including bucket number, bitmap size (bmsize), and bitmap page found status (bmpage_found)

## Dependencies
- Functions called/Symbols referenced:
  - [xl_hash_add_ovfl_page](../x/xl_hash_add_ovfl_page.md) (WAL record structure)
  - XLogRecGetData (extracts record data)
  - [XLogRecGetBlockTag](../X/XLogRecGetBlockTag.md) (gets block information)
  - [XLogInitBufferForRedo](../X/XLogInitBufferForRedo.md) (initializes buffer for redo)
  - [XLogRecGetBlockData](../X/XLogRecGetBlockData.md) (gets block data)
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md) (reads buffer for redo)
  - XLogRecHasBlockRef (checks if record has block reference)
  - [_hash_initbuf](_hash_initbuf.md) (initializes hash page buffer)
  - [_hash_initbitmapbuffer](_hash_initbitmapbuffer.md) (initializes bitmap buffer)
  - HashPageGetOpaque (gets page opaque data)
  - HashPageGetBitmap (gets bitmap from page)
  - HashPageGetMeta (gets metapage metadata)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md) (gets buffer block number)
  - BLK_NEEDS_REDO (indicates block needs redo)
  - LH_OVERFLOW_PAGE (overflow page type)
  - SETBIT (sets bit in bitmap)
  - BlockNumberIsValid (validates block number)
- Called from:
  - [hash_redo](hash_redo.md) (main hash WAL replay function)

## Notes and Other Information
- This is a static function used only within the hash WAL recovery subsystem
- The function manages complex page linkage by updating hasho_prevblkno and hasho_nextblkno pointers
- During normal operation, all pages would be locked simultaneously, but during replay concurrent access isn't possible
- The function may create new bitmap pages if existing ones are full, updating the metapage's bitmap registry
- Metapage updates include managing hashm_firstfree, hashm_spares, hashm_mapp, and hashm_nmaps fields
- The function handles conditional operations based on whether bitmap pages were found during the original operation
- Bitmap allocation tracking ensures proper overflow page management and prevents page leaks