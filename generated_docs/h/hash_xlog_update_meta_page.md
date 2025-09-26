# hash_xlog_update_meta_page

## Location
[src/backend/access/hash/hash_xlog.c:964-990](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hash_xlog.c#L964-L990)

## Overview
This function replays updates to the hash index meta page during PostgreSQL WAL recovery, specifically updating the tuple count stored in the meta page.

## Definition
```c
static void hash_xlog_update_meta_page(XLogReaderState *record)
```

## Detailed Description
The hash_xlog_update_meta_page function is a WAL replay handler that processes meta page updates during crash recovery for hash indexes. The meta page contains critical metadata about the hash index, including the total number of tuples. This function extracts the tuple count from the WAL record and updates the meta page accordingly.

During replay, the function reads the meta page buffer, extracts the xl_hash_update_meta_page data structure from the WAL record, and updates the hashm_ntuples field in the meta page. This ensures that the hash index metadata remains consistent after recovery.

## Parameters / Member Variables
- `record`: XLogReaderState pointer containing the WAL record being replayed, which includes the xl_hash_update_meta_page data structure with the new tuple count

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md)
  - [BufferGetPage](../B/BufferGetPage.md)
  - HashPageGetMeta
  - [PageSetLSN](../P/PageSetLSN.md)
  - [MarkBufferDirty](../M/MarkBufferDirty.md)
  - [BufferIsValid](../B/BufferIsValid.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
- Types referenced:
  - HashMetaPage
  - [xl_hash_update_meta_page](../x/xl_hash_update_meta_page.md)
  - BLK_NEEDS_REDO
- Called from:
  - [hash_redo](hash_redo.md)

## Notes and Other Information
- This is a static function used only within the hash WAL recovery subsystem
- The function specifically updates the hashm_ntuples field, which tracks the total number of tuples in the hash index
- Meta page updates are critical for maintaining accurate statistics and ensuring proper hash index behavior
- The xl_hash_update_meta_page structure contains the WAL record data with the new tuple count
- Buffer management follows standard PostgreSQL patterns to prevent resource leaks during recovery