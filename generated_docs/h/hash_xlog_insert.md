# hash_xlog_insert

## Location
[src/backend/access/hash/hash_xlog.c:125-172](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hash_xlog.c#L125-L172)

## Overview
Replays a hash index tuple insertion operation during WAL recovery, including updating both the data page and the metapage tuple count.

## Definition

```c
static void
hash_xlog_insert(XLogReaderState *record)
```
## Detailed Description
This function handles WAL replay for hash index tuple insertions that don't require page splits. During normal hash index operations, when a new tuple is inserted, the operation is logged to WAL. During recovery, this function reconstructs the insertion by reading the tuple data from the WAL record and inserting it into the appropriate page at the specified offset.

The function operates on two buffers: first, it inserts the tuple data into the target data page using PageAddItem; second, it updates the metapage to increment the total tuple count (hashm_ntuples). The function includes error handling to ensure the insertion succeeds, panicking if PageAddItem fails.

## Parameters / Member Variables
- : XLogReaderState pointer containing the WAL record with insertion data including the tuple data and target offset number (offnum)

## Dependencies
- Functions called/Symbols referenced:
  - [xl_hash_insert](../x/xl_hash_insert.md) (WAL record structure)
  - XLogRecGetData (extracts record data)
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md) (reads buffers for redo operations)
  - [XLogRecGetBlockData](../X/XLogRecGetBlockData.md) (gets block data from record)
  - BLK_NEEDS_REDO (indicates block needs redo)
  - PageAddItem (adds tuple to page)
  - HashPageGetMeta (gets metapage metadata)
  - Item (tuple data type)
  - InvalidOffsetNumber (invalid offset constant)
  - PANIC (error level constant)
- Called from:
  - [hash_redo](hash_redo.md) (main hash WAL replay function)

## Notes and Other Information
- This is a static function used only within the hash WAL recovery subsystem
- The function handles insertions that don't cause page splits - split operations are handled separately
- During normal operation, both the data page and metapage would be locked simultaneously, but during replay this isn't necessary due to no concurrent access
- The function panics if PageAddItem fails, indicating a serious consistency problem
- The metapage's hashm_ntuples field is incremented to maintain accurate tuple count statistics
- Two separate buffer operations ensure atomicity - the insertion and the metapage update are handled independently