# hash_xlog_vacuum_one_page

## Location
src/backend/access/hash/hash_xlog.c: 991 - 1066

## Overview
This function replays vacuum operations on a single hash index page during WAL recovery, removing tuples marked as DEAD and updating the meta page tuple count accordingly.

## Definition
```c
static void hash_xlog_vacuum_one_page(XLogReaderState *record)
```

## Detailed Description
The hash_xlog_vacuum_one_page function is a WAL replay handler that processes vacuum operations during crash recovery for hash indexes. It removes tuples that were marked as DEAD during index tuple insertion, which is a cleanup operation that happens as part of hash index maintenance.

The function performs several key operations:
1. Handles hot standby conflicts by resolving snapshot conflicts before updating pages
2. Removes the dead tuples from the target page using PageIndexMultiDelete
3. Clears the LH_PAGE_HAS_DEAD_TUPLES flag from the page opaque data
4. Updates the meta page to decrement the tuple count by the number of removed tuples

Hot standby conflict resolution is important because removing DEAD tuples can conflict with read queries on standby servers that might still need to see those tuples based on their snapshot.

## Parameters / Member Variables
- `record`: XLogReaderState pointer containing the WAL record with xl_hash_vacuum_one_page data structure, including the offsets of tuples to delete and conflict resolution information

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - [XLogRecGetBlockTag](../X/XLogRecGetBlockTag.md)
  - [ResolveRecoveryConflictWithSnapshot](../R/ResolveRecoveryConflictWithSnapshot.md)
  - XLogReadBufferForRedoExtended
  - [BufferGetPage](../B/BufferGetPage.md)
  - [PageIndexMultiDelete](../P/PageIndexMultiDelete.md)
  - HashPageGetOpaque
  - HashPageGetMeta
  - [PageSetLSN](../P/PageSetLSN.md)
  - MarkBufferDirty
  - [BufferIsValid](../B/BufferIsValid.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
  - XLogReadBufferForRedo
- Types referenced:
  - [xl_hash_vacuum_one_page](../x/xl_hash_vacuum_one_page.md)
  - XLogRedoAction
  - HashPageOpaque
  - HashMetaPage
  - [RelFileLocator](../R/RelFileLocator.md)
  - BLK_NEEDS_REDO
  - RBM_NORMAL
  - LH_PAGE_HAS_DEAD_TUPLES
- Constants referenced:
  - InHotStandby
- Called from:
  - [hash_redo](hash_redo.md)

## Notes and Other Information
- This is a static function used only within the hash WAL recovery subsystem
- The function handles both the target page (where tuples are removed) and the meta page (where tuple count is updated)
- Hot standby conflict resolution is performed to ensure consistency with read queries on standby servers
- The LH_PAGE_HAS_DEAD_TUPLES flag is cleared after vacuum to indicate the page no longer contains dead tuples
- The function updates two buffers: the target page buffer and the meta page buffer
- Proper buffer management ensures no resource leaks during recovery
- The xl_hash_vacuum_one_page structure contains the offsets array of tuples to delete and conflict resolution information